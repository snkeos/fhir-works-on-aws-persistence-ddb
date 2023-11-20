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
} from 'fhir-works-on-aws-interface';

import { Buffer } from 'buffer';
import assert from 'assert';
import { DynamoDbDataService } from './dynamoDbDataService';
import { DynamoDbUtil } from './dynamoDbUtil';

import S3ObjectStorageService from '../objectStorageService/s3ObjectStorageService';
import { SEPARATOR } from '../constants';

export const decode = (str: string): string => Buffer.from(str, 'base64').toString('utf-8');
export const encode = (str: string): string => Buffer.from(str, 'utf-8').toString('base64');

export type StripPayloadFunction = {
    (resource: any): any;
};

export class HybridDataService implements Persistence, BulkDataAccess {
    updateCreateSupported: boolean = false;

    private resourceTypesToStoreOnObjectStorage: Map<string, StripPayloadFunction> = new Map<
        string,
        StripPayloadFunction
    >();

    readonly enableMultiTenancy: boolean;

    private readonly dbPersistenceService: DynamoDbDataService;

    private static async replaceStrippedResourceWithS3Version(strippedResource: any): Promise<any | undefined> {
        if (strippedResource?.meta?.source) {
            try {
                const readObjectResult = await S3ObjectStorageService.readObject(strippedResource.meta.source);
                const resourceFromS3 = JSON.parse(decode(readObjectResult.message));
                resourceFromS3.meta = strippedResource.meta;
                delete resourceFromS3.meta.source;
                return resourceFromS3;
            } catch (e) {
                throw new ResourceNotFoundError(strippedResource.resourceType, strippedResource.id);
            }
        }
        return undefined;
    }

    static async cleanItemAndCompose(item: any) {
        const cleanedResource = DynamoDbUtil.cleanItem(item);
        const replaceObjectResult = await HybridDataService.replaceStrippedResourceWithS3Version(cleanedResource);
        return replaceObjectResult || cleanedResource;
    }

    constructor(
        dbPersistenceService: DynamoDbDataService,
        { enableMultiTenancy = false }: { enableMultiTenancy?: boolean } = {},
    ) {
        this.dbPersistenceService = dbPersistenceService;
        this.enableMultiTenancy = enableMultiTenancy;
    }

    registerToStoreOnObjectStorage(resourceType: string, fn: StripPayloadFunction): void {
        this.resourceTypesToStoreOnObjectStorage.set(resourceType, fn);
    }

    private shallStoreOnObjectStorage(resourceType: string): boolean {
        return this.resourceTypesToStoreOnObjectStorage.has(resourceType);
    }

    private stripPayloadFromResource(resourceType: string, resource: any): any {
        const func = this.resourceTypesToStoreOnObjectStorage.get(resourceType);
        if (func) {
            return func(resource);
        }
        return resource;
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
        const replaceObjectResult = await HybridDataService.replaceStrippedResourceWithS3Version(getResponse.resource);
        if (replaceObjectResult) {
            return {
                message: getResponse.message,
                resource: replaceObjectResult,
            };
        }
        return getResponse;
    }

    async vReadResource(request: vReadResourceRequest): Promise<GenericResponse> {
        this.assertValidTenancyMode(request.tenantId);
        const getResponse = await this.dbPersistenceService.vReadResource(request);
        const replaceObjectResult = await HybridDataService.replaceStrippedResourceWithS3Version(getResponse.resource);
        if (replaceObjectResult) {
            return {
                message: getResponse.message,
                resource: replaceObjectResult,
            };
        }
        return getResponse;
    }

    async createResource(request: CreateResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);

        const newResourceId = uuidv4();
        const { resourceType, resource, tenantId } = request;

        if (this.shallStoreOnObjectStorage(request.resourceType)) {
            const fileName = this.getPathName(newResourceId, uuidv4(), resourceType, tenantId);

            const resourceClone = clone(resource);
            resourceClone.id = newResourceId;
            const base64Data = encode(JSON.stringify(resourceClone));

            // remove the main payload.
            const strippedResource = this.stripPayloadFromResource(request.resourceType, clone(request.resource));

            // link the s3 key to the stripped resource
            if (!strippedResource.meta) {
                strippedResource.meta = { source: fileName };
            } else {
                strippedResource.meta = { ...strippedResource.meta, source: fileName };
            }
            try {
                const [createResponse, s3UploadResult] = await Promise.all([
                    this.dbPersistenceService.createResourceWithId(
                        resourceType,
                        strippedResource,
                        newResourceId,
                        tenantId,
                    ),
                    S3ObjectStorageService.uploadObject(base64Data, fileName, 'application/json'),
                ]);
                resourceClone.meta = createResponse.resource.meta;
                assert(s3UploadResult.message, '');
                return {
                    success: createResponse.success,
                    message: createResponse.message,
                    resource: resourceClone,
                };
            } catch (e) {
                await this.dbPersistenceService.deleteResource({
                    resourceType: request.resourceType,
                    id: strippedResource.id,
                });
                throw e;
            }
        } else {
            return this.dbPersistenceService.createResourceWithId(resourceType, resource, newResourceId, tenantId);
        }
    }

    async updateResource(request: UpdateResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, id, tenantId } = request;

        if (this.shallStoreOnObjectStorage(request.resourceType)) {
            await this.dbPersistenceService.readResource(request);
            const fileName = this.getPathName(id, uuidv4(), resourceType, tenantId);
            const resourceClone = clone(request.resource);
            const base64Data = encode(JSON.stringify(resourceClone));
            // remove the main payload.
            const strippedResource = this.stripPayloadFromResource(request.resourceType, clone(request.resource));

            // link the s3 key to the stripped resource
            if (!strippedResource.meta) {
                strippedResource.meta = { source: fileName };
            } else {
                strippedResource.meta = { ...strippedResource.meta, source: fileName };
            }

            try {
                const [updateResponse, s3UploadResult] = await Promise.all([
                    this.dbPersistenceService.updateResourceWithOutCheck(resourceType, strippedResource, id, tenantId),
                    S3ObjectStorageService.uploadObject(base64Data, fileName, 'application/json'),
                ]);
                resourceClone.meta = updateResponse.resource.meta;
                assert(s3UploadResult.message, '');
                return {
                    success: updateResponse.success,
                    message: updateResponse.message,
                    resource: resourceClone,
                };
            } catch (e) {
                await this.dbPersistenceService.deleteResource({ resourceType: request.resourceType, id });
                throw e;
            }
        } else {
            return this.dbPersistenceService.updateResource(request);
        }
    }

    async deleteResource(request: DeleteResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, id, tenantId } = request;
        const itemServiceResponse = await this.dbPersistenceService.readResource({ resourceType, id, tenantId });
        const { versionId, source } = itemServiceResponse.resource.meta;

        if (source) {
            const [, deleteResponse] = await Promise.all([
                S3ObjectStorageService.deleteObject(source),
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
