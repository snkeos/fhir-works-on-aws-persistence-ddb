/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

/* eslint-disable class-methods-use-this */

import uuidv4 from 'uuid/v4';
import {
    BulkDataAccess,
    GenericResponse,
    Persistence,
    clone,
    ReadResourceRequest,
    vReadResourceRequest,
    CreateResourceRequest,
    DeleteResourceRequest,
    UpdateResourceRequest,
    PatchResourceRequest,
    ConditionalDeleteResourceRequest,
    InitiateExportRequest,
    GetExportStatusResponse,
    ResourceNotFoundError,
    isResourceNotFoundError,
} from 'fhir-works-on-aws-interface';

import { Buffer } from 'buffer';
import { DynamoDbDataService } from './dynamoDbDataService';
import S3ObjectStorageService from '../objectStorageService/s3ObjectStorageService';
import { SEPARATOR } from '../constants';

export const decode = (str: string): string => Buffer.from(str, 'base64').toString('utf-8');
export const encode = (str: string): string => Buffer.from(str, 'utf-8').toString('base64');

interface ILoadHandler {
    get: (link: string) => any;
    push: (load: any) => any;
    delete: (link: string) => any;
}

class LoadHandler implements ILoadHandler {
    async get(link: string) {
        const response = await S3ObjectStorageService.readObject(link);
        const load = JSON.parse(decode(response.message));
        if (load.link != link) throw new Error('S3 bulk data does not match');
        return load;
    }

    async push(load: any) {
        const message = encode(JSON.stringify(load));
        return await S3ObjectStorageService.uploadObject(message, load.link, 'application/json',);
    }

    async delete(link: string) {
        try {
            await S3ObjectStorageService.deleteObject(link);
        } catch {
            console.log(`Failed to delete load [${link}] from S3`);
        }
    }
}

export class HybridDataService implements Persistence, BulkDataAccess {
    updateCreateSupported: boolean = false;

    // private resourceTypesToStoreOnObjectStorage: Map<string, Array<string>> = new Map<string, Array<string>>();
    private attributesToDeloadForResourceType = new Map<string, Array<string>>();

    readonly enableMultiTenancy: boolean;

    private readonly dbPersistenceService: DynamoDbDataService;

    private readonly loadHandler: ILoadHandler = new LoadHandler;

    private static makeLoadLink(id: string, versionId: string, resourceType: string, tenantId: string = '') {
        const fileExtension = 'json';
        const filename = `${resourceType}/${id}${SEPARATOR}${versionId}.${fileExtension}`;
        return tenantId ? `${tenantId}/${filename}` : filename;
    }

    private static attachLoad(resource: any, load: any) {
        Object.entries(load.data).forEach(([key, value]) => {
            // eslint-disable-next-line no-param-reassign
            resource[key] = value;
        });
        // eslint-disable-next-line no-param-reassign
        delete resource.bulkDataLink;
        return resource;
    }

    private static detachLoad(resource: any, loadLink: string, fieldsToDetach: Array<string>) {
        // This code shall reduce the cloning overhead, by temporary remove the main payload.
        const load: any = { link: loadLink, data: {} };
        let unloadedResource = clone(resource);
        for (let field of fieldsToDetach) {
            load.data[field] = resource[field];
            delete unloadedResource[field];
        };

        unloadedResource.bulkDataLink = loadLink;
        return {
            unloadedResource: unloadedResource,
            load: load,
        };
    }

    constructor(
        dbPersistenceService: DynamoDbDataService,
        { enableMultiTenancy = false }: { enableMultiTenancy?: boolean } = {},
    ) {
        this.dbPersistenceService = dbPersistenceService;
        this.enableMultiTenancy = enableMultiTenancy;
    }

    registerToStoreOnObjectStorage(resourceType: string, attributes: Array<string>): void {
        this.attributesToDeloadForResourceType.set(resourceType, attributes);
    }

    private shallBeDeloaded(resourceType: string): boolean {
        return this.attributesToDeloadForResourceType.has(resourceType);
    }

    private assertValidTenancyMode(tenantId?: string) {
        if (this.enableMultiTenancy && tenantId === undefined) {
            throw new Error('This instance has multi-tenancy enabled, but the incoming request is missing tenantId');
        }
        if (!this.enableMultiTenancy && tenantId !== undefined) {
            throw new Error('This instance has multi-tenancy disabled, but the incoming request has a tenantId');
        }
    }

    private async getCompleteResource(resource: any, loadLink: string) {
        try {
            const load = this.loadHandler.get(loadLink);
            return HybridDataService.attachLoad(resource, load);
        } catch {
            console.log(`Cannot extract payload for the resource: id = ${resource.id}, link = ${loadLink}`);
            return resource;
        }
    }

    async readResource(request: ReadResourceRequest): Promise<GenericResponse> {
        this.assertValidTenancyMode(request.tenantId);
        const response = await this.dbPersistenceService.readResource(request);
        const resource = response.resource
        if (!this.shallBeDeloaded(resource.resourceType)) {
            return resource;
        }
        return {
            message: response.message,
            resource: await this.getCompleteResource(resource, resource.resourceType),
        };
    }

    async vReadResource(request: vReadResourceRequest): Promise<GenericResponse> {
        this.assertValidTenancyMode(request.tenantId);
        const response = await this.dbPersistenceService.vReadResource(request);
        const resource = response.resource
        if (!this.shallBeDeloaded(resource.resourceType)) {
            return resource;
        }
        return {
            message: response.message,
            resource: await this.getCompleteResource(resource, resource.resourceType),
        };
    }

    async createResource(request: CreateResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resource, resourceType, tenantId } = request;
        const resourceId = uuidv4();
        return await this.createResourceWithId(resourceType, resource, resourceId, tenantId)
    }

    async createResourceWithId(resourceType: string, resource: any, resourceId: string, tenantId?: string) {
        this.assertValidTenancyMode(tenantId);
        if (this.shallBeDeloaded(resourceType)) {
            return await this.createResourceWithLoadBackup(
                resourceType, resource, resourceId, tenantId);
        } else {
            return await this.dbPersistenceService.createResourceWithId(
                resourceType, resource, resourceId, tenantId);
        }
    }

    private async createResourceWithLoadBackup(resourceType: string, resource: any, resourceId: string, tenantId?: string) {
        const loadLink = HybridDataService.makeLoadLink(resourceId, uuidv4(), resourceType, tenantId);
        const { unloadedResource, load } = HybridDataService.detachLoad(
            resource, loadLink,
            this.attributesToDeloadForResourceType.get(resourceType)!
        );
        await this.loadHandler.push(load);
        unloadedResource.BuldDataLink = loadLink;
        try {
            const createResponse = await this.dbPersistenceService.createResourceWithIdNoClone(
                resourceType, unloadedResource, resourceId, tenantId);

            const resourceToReturn = clone(resource);
            resourceToReturn.meta = createResponse.resource.meta;
            return {
                success: createResponse.success,
                message: createResponse.message,
                resource: resourceToReturn,
            };
        } catch (e) {
            console.log(`Creation of ${resource.resourceType}: ${resource.id} has failed.`);
            await this.loadHandler.delete(loadLink);
            throw e;
        }
    }

    async updateResource(request: UpdateResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, resource, id, tenantId } = request;
        let readResponse; // forward declaration
        try {
            // Will throw ResourceNotFoundError if resource can't be found
            readResponse = await this.dbPersistenceService.readResource({ resourceType, id, tenantId });
        } catch (e) {
            if (this.updateCreateSupported && isResourceNotFoundError(e)) {
                return this.createResourceWithId(resourceType, resource, id, tenantId);
            }
            throw e;
        }
        if (this.shallBeDeloaded(resourceType)) {
            return await this.dbPersistenceService.updateResource(request);
        } else {
            const updateResponse = await this.updateResourceWithLoadBackup(resourceType, resource, id, tenantId);
            const oldLoadLink = readResponse.resource?.BulkDataLink
            if (oldLoadLink) {
                await this.loadHandler.delete(oldLoadLink);
            }
            return updateResponse;
        }
    }

    private async updateResourceWithLoadBackup(resourceType: string, resource: any, resourceId: string, tenantId?: string) {
        const loadLink = HybridDataService.makeLoadLink(resourceId, uuidv4(), resourceType, tenantId);
        const { unloadedResource, load } = HybridDataService.detachLoad(
            resource, loadLink,
            this.attributesToDeloadForResourceType.get(resourceType)!
        );
        await this.loadHandler.push(load);
        unloadedResource.BuldDataLink = loadLink;
        try {
            const updateResponse = await this.dbPersistenceService.updateResourceNoCheckForExistenceNoClone(
                resourceType, unloadedResource, resourceId, tenantId);

            const resourceToReturn = clone(resource);
            resourceToReturn.meta = updateResponse.resource.meta;

            return {
                success: updateResponse.success,
                message: updateResponse.message,
                resource: resourceToReturn,
            };
        } catch (e) {
            console.log(`Failed to update the resource: type =  ${resource.resourceType}, id = ${resource.id}.`);
            await this.loadHandler.delete(loadLink);
            throw e;
        }
    }

    async deleteResource(request: DeleteResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, id, tenantId } = request;
        const itemServiceResponse = await this.dbPersistenceService.readResource({ resourceType, id, tenantId });
        const { versionId } = itemServiceResponse.resource.meta;
        const { loadLink } = itemServiceResponse.resource;
        if (loadLink) {
            const [, deleteResponse] = await Promise.all([
                this.loadHandler.delete(loadLink),
                this.dbPersistenceService.deleteVersionedResource(id, parseInt(versionId, 10), tenantId),
            ]);
            return deleteResponse;
        }
        return this.dbPersistenceService.deleteVersionedResource(id, parseInt(versionId, 10), tenantId);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    conditionalCreateResource(request: CreateResourceRequest, queryParams: any): Promise<GenericResponse> {
        return this.dbPersistenceService.conditionalCreateResource(request, queryParams);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    conditionalUpdateResource(request: UpdateResourceRequest, queryParams: any): Promise<GenericResponse> {
        return this.dbPersistenceService.conditionalUpdateResource(request, queryParams);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    patchResource(request: PatchResourceRequest): Promise<GenericResponse> {
        return this.dbPersistenceService.patchResource(request);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    conditionalPatchResource(request: PatchResourceRequest, queryParams: any): Promise<GenericResponse> {
        return this.dbPersistenceService.conditionalUpdateResource(request, queryParams);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    conditionalDeleteResource(request: ConditionalDeleteResourceRequest, queryParams: any): Promise<GenericResponse> {
        return this.dbPersistenceService.conditionalDeleteResource(request, queryParams);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    async initiateExport(initiateExportRequest: InitiateExportRequest): Promise<string> {
        return this.dbPersistenceService.initiateExport(initiateExportRequest);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    async cancelExport(jobId: string): Promise<void> {
        return this.dbPersistenceService.cancelExport(jobId);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    async getExportStatus(jobId: string): Promise<GetExportStatusResponse> {
        return this.dbPersistenceService.getExportStatus(jobId);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    async getActiveSubscriptions(params: { tenantId?: string }): Promise<Record<string, any>[]> {
        return this.dbPersistenceService.getActiveSubscriptions(params);
    }

}
