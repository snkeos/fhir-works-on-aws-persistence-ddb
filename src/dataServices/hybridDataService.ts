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
} from 'fhir-works-on-aws-interface';

import { Buffer } from 'buffer';
import assert from 'assert';
import { DynamoDbDataService } from './dynamoDbDataService';
import { DynamoDbUtil } from './dynamoDbUtil';

import S3ObjectStorageService from '../objectStorageService/s3ObjectStorageService';
import { SEPARATOR } from '../constants';

import getComponentLogger from '../loggerBuilder';

const logger = getComponentLogger();

export const decode = (str: string): string => Buffer.from(str, 'base64').toString('binary');
export const encode = (str: string): string => Buffer.from(str, 'binary').toString('base64');

export class HybridDataService implements Persistence, BulkDataAccess {
    updateCreateSupported: boolean = false;

    readonly enableMultiTenancy: boolean;

    private readonly dbPersistenceService: DynamoDbDataService;

    static async cleanItemAndCompose(item: any) {
        const cleanedResource = DynamoDbUtil.cleanItem(item);
        if (cleanedResource?.meta?.source) {
            const readObjectResult = await S3ObjectStorageService.readObject(cleanedResource?.meta?.source);
            return JSON.parse(decode(readObjectResult.message));
        }
        return cleanedResource;
    }

    constructor(
        dbPersistenceService: DynamoDbDataService,
        { enableMultiTenancy = false }: { enableMultiTenancy?: boolean } = {},
    ) {
        this.dbPersistenceService = dbPersistenceService;
        this.enableMultiTenancy = enableMultiTenancy;
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

        if (getResponse.resource?.meta?.source) {
            logger.log('Stripped resoure found');
            logger.log(`Start loading resoure from: ${getResponse.resource.meta.source}`);
            const readObjectResult = await S3ObjectStorageService.readObject(getResponse.resource.meta.source);
            logger.log(`${getResponse.resource.meta.source} loaded.`);

            try {
                return {
                    message: getResponse.message,
                    resource: JSON.parse(readObjectResult.message),
                };
            } catch (error) {
                logger.log(`Failed to parse the resource from S3 ${error}`);
                throw error;
            }
        }
        return getResponse;
    }

    async vReadResource(request: vReadResourceRequest): Promise<GenericResponse> {
        this.assertValidTenancyMode(request.tenantId);
        const getResponse = await this.dbPersistenceService.vReadResource(request);
        if (getResponse.resource?.meta?.source) {
            const readObjectResult = await S3ObjectStorageService.readObject(getResponse.resource?.meta?.source);
            return {
                message: getResponse.message,
                resource: JSON.parse(readObjectResult.message),
            };
        }
        return getResponse;
    }

    async createResource(request: CreateResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);

        const newResourceId = uuidv4();
        const { resourceType, resource, tenantId } = request;

        if (request.resourceType === `Questionnaire`) {
            const fileName = this.getPathName(newResourceId, uuidv4(), resourceType, tenantId);

            const resourceClone = clone(resource);
            resourceClone.id = newResourceId;

            // remove the main payload.
            delete resource.item;

            if (!resource.meta) {
                resource.meta = { source: fileName };
            } else {
                resource.meta = { ...resource.meta, source: fileName };
            }
            try {
                const [createResponse, s3UploadResult] = await Promise.all([
                    this.dbPersistenceService.createResourceWithId(resourceType, resource, newResourceId, tenantId),
                    S3ObjectStorageService.uploadObject(JSON.stringify(resourceClone), fileName, 'application/json'),
                ]);
                resourceClone.meta = createResponse.resource.meta;
                assert(s3UploadResult.message, '');
                return {
                    success: createResponse.success,
                    message: createResponse.message,
                    resource: resourceClone,
                };
            } catch (e) {
                await this.dbPersistenceService.deleteResource({ resourceType: request.resourceType, id: resource.id });
                throw e;
            }
        } else {
            return this.dbPersistenceService.createResourceWithId(resourceType, resource, newResourceId, tenantId);
        }
    }

    async updateResource(request: UpdateResourceRequest) {
        this.assertValidTenancyMode(request.tenantId);
        const { resourceType, id, tenantId } = request;

        if (request.resourceType === `Questionnaire`) {
            const getResponse = await this.dbPersistenceService.readResource(request);
            let fileName = '';
            if (getResponse.resource?.meta?.source) {
                fileName = getResponse.resource?.meta?.source;
            } else {
                fileName = this.getPathName(id, uuidv4(), resourceType, tenantId);
            }
            const resourceClone = clone(request.resource);
            // remove the main payload.
            delete request.resource.item;

            if (!request.resource.meta) {
                request.resource.meta = { source: fileName };
            } else {
                request.resource.meta = { ...request.resource.meta, source: fileName };
            }

            try {
                const [updateResponse, s3UploadResult] = await Promise.all([
                    this.dbPersistenceService.updateResourceWithOutCheck(request),
                    S3ObjectStorageService.uploadObject(
                        encode(JSON.stringify(resourceClone)),
                        fileName,
                        'application/json',
                    ),
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
        const itemServiceResponse = await this.readResource({ resourceType, id, tenantId });
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
