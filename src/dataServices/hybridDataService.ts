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

export class HybridDataService implements Persistence, BulkDataAccess {
    updateCreateSupported: boolean = false;

    private attributesToDeloadForResourceType: Map<string, Array<string>> = new Map<string, Array<string>>();

    readonly enableMultiTenancy: boolean;

    private readonly dbPersistenceService: DynamoDbDataService;

    static async attachPayloadToResource(resource: any): Promise<any> {
        if (resource?.bulkDataLink) {
            try {
                const readObjectResult = await S3ObjectStorageService.readObject(resource.bulkDataLink);
                const bulkDataFromS3 = JSON.parse(decode(readObjectResult.message));
                if (bulkDataFromS3.link === resource.bulkDataLink) {
                    Object.entries(bulkDataFromS3.data).forEach(([key, value]) => {
                        // eslint-disable-next-line no-param-reassign
                        resource[key] = value;
                    });
                    // eslint-disable-next-line no-param-reassign
                    delete resource.bulkDataLink;
                    return resource;
                }
                console.log(`S3 bulk data does not match`);
                throw new Error(`S3 bulk data does not match`);
            } catch (e) {
                console.log(`Load ${resource.resourceType}: ${resource.id} from S3 failed`);
                throw new ResourceNotFoundError(resource.resourceType, resource.id);
            }
        }
        return resource;
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

    private shallStoreOnObjectStorage(resourceType: string): boolean {
        return this.attributesToDeloadForResourceType.has(resourceType);
    }

    private detachPayloadFromResource(resourceType: string, resource: any, resourceId: string, tenantId?: string): any {
        const attributes = this.attributesToDeloadForResourceType.get(resourceType);
        if (attributes) {
            // Create a link for S3 object storage
            const bulkDataLink = this.getPathName(resourceId, uuidv4(), resourceType, tenantId);

            // This code shall reduce the cloning overhead, by temporary remove the main payload.
            const attributesToRestore = attributes.map((element) => {
                const payload = resource[element];
                // eslint-disable-next-line no-param-reassign
                delete resource[element];
                return {
                    name: element,
                    data: payload,
                };
            });
            // clone the temporary stripped resource
            const resourceClone = clone(resource);
            const bulkData: any = { link: bulkDataLink, data: {} };
            // Restore the original JSON and fill the bulk data structure
            attributesToRestore.forEach((element) => {
                // eslint-disable-next-line no-param-reassign
                resource[element.name] = element.data;

                bulkData.data[element.name] = element.data;
            });
            // link the s3 key to the stripped resource
            resourceClone.bulkDataLink = bulkDataLink;
            return {
                resource: resourceClone,
                bulkDataLink,
                bulkData: encode(JSON.stringify(bulkData)),
            };
        }
        return { resource };
    }

    private assertValidTenancyMode(tenantId?: string) {
        if (this.enableMultiTenancy && tenantId === undefined) {
            throw new Error('This instance has multi-tenancy enabled, but the incoming request is missing tenantId');
        }
        if (!this.enableMultiTenancy && tenantId !== undefined) {
            throw new Error('This instance has multi-tenancy disabled, but the incoming request has a tenantId');
        }
    }

    async readResource(request: ReadResourceRequest): Promise<GenericResponse> {
        this.assertValidTenancyMode(request.tenantId);
        const getResponse = await this.dbPersistenceService.readResource(request);
        return {
            message: getResponse.message,
            resource: await HybridDataService.attachPayloadToResource(getResponse.resource),
        };
    }

    async vReadResource(request: vReadResourceRequest): Promise<GenericResponse> {
        this.assertValidTenancyMode(request.tenantId);
        const getResponse = await this.dbPersistenceService.vReadResource(request);
        return {
            message: getResponse.message,
            resource: await HybridDataService.attachPayloadToResource(getResponse.resource),
        };
    }

    async createResource(request: CreateResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, resource, tenantId } = request;
        return this.createResourceWithId(resourceType, resource, uuidv4(), tenantId);
    }

    async createResourceWithId(resourceType: string, resource: any, resourceId: string, tenantId?: string) {
        if (!this.shallStoreOnObjectStorage(resourceType)) {
            return this.dbPersistenceService.createResourceWithId(resourceType, resource, resourceId, tenantId);
        }
        const resourceClone = clone(resource);
        resourceClone.id = resourceId;

        // Separate the main payload from the resource.
        const detachedResourceData = this.detachPayloadFromResource(resourceType, resourceClone, resourceId, tenantId);

        // Ensure the order: first S3 then ddb to avoid possible data races
        await S3ObjectStorageService.uploadObject(
            detachedResourceData.bulkData,
            detachedResourceData.bulkDataLink,
            'application/json',
        );
        try {
            const createResponse = await this.dbPersistenceService.createResourceWithIdNoClone(
                resourceType,
                detachedResourceData.resource,
                resourceId,
                tenantId,
            );
            // Update the meta data
            resourceClone.meta = createResponse.resource.meta;
            return {
                success: createResponse.success,
                message: createResponse.message,
                resource: resourceClone,
            };
        } catch (e) {
            console.log(`Creation of ${resource.resourceType}: ${resource.id} has failed`);
            await S3ObjectStorageService.deleteObject(detachedResourceData.bulkDataLink);
            throw e;
        }
    }

    async updateResource(request: UpdateResourceRequest) {
        if (!this.shallStoreOnObjectStorage(request.resourceType)) {
            return this.dbPersistenceService.updateResource(request);
        }
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, resource, id, tenantId } = request;
        try {
            // Will throw ResourceNotFoundError if resource can't be found
            await this.dbPersistenceService.readResource({ resourceType, id, tenantId });
        } catch (e) {
            if (this.updateCreateSupported && isResourceNotFoundError(e)) {
                return this.createResourceWithId(resourceType, resource, id, tenantId);
            }
            throw e;
        }
        const resourceClone = clone(resource);
        // Separate the main payload from the resource.
        const detachedResourceData = this.detachPayloadFromResource(resourceType, resourceClone, id, tenantId);
        // Ensure the order: first S3 then ddb to avoid possible data races
        await S3ObjectStorageService.uploadObject(
            detachedResourceData.bulkData,
            detachedResourceData.bulkDataLink,
            'application/json',
        );
        try {
            const updateResponse = await this.dbPersistenceService.updateResourceNoCheckForExistenceNoClone(
                resourceType,
                detachedResourceData.resource,
                id,
                tenantId,
            );
            // Update the meta data
            resourceClone.meta = updateResponse.resource.meta;

            return {
                success: updateResponse.success,
                message: updateResponse.message,
                resource: resourceClone,
            };
        } catch (e) {
            console.log(`Update of ${resource.resourceType}: ${resource.id} has failed`);
            await S3ObjectStorageService.deleteObject(detachedResourceData.bulkDataLink);
            throw e;
        }
    }

    async deleteResource(request: DeleteResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, id, tenantId } = request;
        const itemServiceResponse = await this.dbPersistenceService.readResource({ resourceType, id, tenantId });
        const { versionId } = itemServiceResponse.resource.meta;
        const { bulkDataLink } = itemServiceResponse.resource;
        if (bulkDataLink) {
            const [, deleteResponse] = await Promise.all([
                S3ObjectStorageService.deleteObject(bulkDataLink),
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

    private getPathName(id: string, versionId: string, resourceType: string, tenantId: string = '') {
        const fileExtension = 'json';
        const filename = `${resourceType}/${id}${SEPARATOR}${versionId}.${fileExtension}`;
        return this.enableMultiTenancy ? `${tenantId}/${filename}` : filename;
    }
}
