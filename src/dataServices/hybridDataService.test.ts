/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */
import AWS from 'aws-sdk';

/* eslint-disable @typescript-eslint/no-unused-vars */
// eslint-disable-next-line max-classes-per-file
import {
    vReadResourceRequest,
    ReadResourceRequest,
    GenericResponse,
    generateMeta,
    ResourceNotFoundError,
} from 'fhir-works-on-aws-interface';
import vaildV4Questionnaire from '../../sampleData/vaildV4Questionnaire.json';
import validV4Patient from '../../sampleData/validV4Patient.json';
import { DynamoDbDataService } from './dynamoDbDataService';
import { HybridDataService, encode, decode } from './hybridDataService';

import S3ObjectStorageService from '../objectStorageService/s3ObjectStorageService';

jest.mock('../objectStorageService/s3ObjectStorageService');

jest.setTimeout(100000);

class TestObjectStorage {
    private static objects: Map<string, Buffer> = new Map<string, Buffer>();

    static async uploadObjectWithError(
        base64Data: string,
        fileName: string,
        contentType: string,
    ): Promise<GenericResponse> {
        throw new Error('Failed uploading binary data to S3');
    }

    static async uploadObject(base64Data: string, fileName: string, contentType: string): Promise<GenericResponse> {
        TestObjectStorage.objects.set(fileName, Buffer.from(base64Data, 'base64'));
        return { message: fileName };
    }

    static async readObject(fileName: string): Promise<GenericResponse> {
        const found = TestObjectStorage.objects.get(fileName);
        if (found) {
            const base64Data = found.toString('base64');
            return { message: base64Data };
        }
        throw new Error('S3 object body is empty');
    }

    static async deleteObject(fileName: string): Promise<GenericResponse> {
        TestObjectStorage.objects.delete(fileName);
        return { message: '' };
    }

    static isEmpty(): boolean {
        return TestObjectStorage.objects.size === 0;
    }

    static numberOfObjects(): Number {
        return TestObjectStorage.objects.size;
    }

    static clear(): void {
        return TestObjectStorage.objects.clear();
    }
}

function filterResourceByProjection(resource: any, projectionExpression?: string) {
    if (projectionExpression) {
        const keyVals = projectionExpression
            .split(',')
            .map((el) => el.trim())
            .map((el) => [el, resource[el]]);
        return Object.fromEntries(keyVals);
    }
    return resource;
}

function mockDynamoDbDataService(dynamoDbDataService: DynamoDbDataService, fileNames: Array<string>): void {
    // eslint-disable-next-line no-param-reassign
    dynamoDbDataService.createResourceWithIdNoClone = jest.fn(
        async (resourceType: string, resource: any, resourceId: string, tenantId?: string) => {
            const resourceCopy: any = { ...resource };
            resourceCopy.id = resourceId;
            return {
                success: true,
                message: 'Resource created',
                resource: resourceCopy,
            };
        },
    );

    // eslint-disable-next-line no-param-reassign
    dynamoDbDataService.vReadResource = jest.fn(async (request: vReadResourceRequest) => {
        if (request.resourceType === `Patient`) {
            const resourceCopy: any = { ...validV4Patient };
            resourceCopy.id = request.id;
            resourceCopy.meta = generateMeta(request.vid);
            return { success: true, message: 'Resource found', resource: resourceCopy };
        }
        const resourceCopy: any = { ...vaildV4Questionnaire };
        delete resourceCopy.item;
        resourceCopy.id = request.id;
        resourceCopy.meta = generateMeta(request.vid);
        resourceCopy.bulkDataLink = fileNames[parseInt(request.vid, 10) - 1];
        return { success: true, message: 'Resource found', resource: resourceCopy };
    });

    // eslint-disable-next-line no-param-reassign
    dynamoDbDataService.readResource = jest.fn(async (request: ReadResourceRequest) => {
        if (request.resourceType === `Patient`) {
            const resourceCopy: any = { ...validV4Patient };
            resourceCopy.id = request.id;
            resourceCopy.meta = generateMeta('1');
            return { success: true, message: 'Resource found', resource: resourceCopy };
        }
        const resourceCopy: any = { ...vaildV4Questionnaire };
        delete resourceCopy.item;
        resourceCopy.id = request.id;
        resourceCopy.meta = generateMeta('1');
        [resourceCopy.bulkDataLink] = fileNames;
        return { success: true, message: 'Resource found', resource: resourceCopy };
    });
    // eslint-disable-next-line no-param-reassign
    dynamoDbDataService.readAllResourceVersions = jest.fn(
        async (request: ReadResourceRequest, projectionExpression?: string): Promise<Array<any>> => {
            return fileNames.map((x: string, index: number) => {
                if (request.resourceType === `Patient`) {
                    const resourceCopy: any = { ...validV4Patient };
                    resourceCopy.id = request.id;
                    resourceCopy.meta = generateMeta(`${index + 1}`);
                    return filterResourceByProjection(resourceCopy, projectionExpression);
                }
                const resourceCopy: any = { ...vaildV4Questionnaire };
                delete resourceCopy.item;
                resourceCopy.id = request.id;
                resourceCopy.meta = generateMeta(`${index + 1}`);
                resourceCopy.bulkDataLink = x;
                return filterResourceByProjection(resourceCopy, projectionExpression);
            });
        },
    );

    // eslint-disable-next-line no-param-reassign
    dynamoDbDataService.updateResourceNoCheckForExistenceNoClone = jest.fn(
        async (resourceType: string, resource: any, id: string, tenantId?: string) => {
            const resourceCopy: any = { ...resource };
            return {
                success: true,
                message: 'Resource updated',
                resource: resourceCopy,
            };
        },
    );

    // eslint-disable-next-line no-param-reassign
    dynamoDbDataService.deleteVersionedResource = jest.fn(async (id: string, vid: number, tenantId?: string) => {
        return {
            success: true,
            message: `Successfully deleted resource Id: ${id}, VersionId: ${vid}`,
        };
    });
}

async function storeQuestionnaireBulkData(resource: any, link: string) {
    const bulkData: any = { link, data: { item: resource.item } };
    await TestObjectStorage.uploadObject(encode(JSON.stringify(bulkData)), link, 'application/json');
}

beforeEach(() => {
    expect.hasAssertions();
});

afterEach(() => {
    TestObjectStorage.clear();
});

describe('Encoding JSON for object storage tests', () => {
    test('Test encode / decode turn around', async () => {
        const sourceJsonString = JSON.stringify(vaildV4Questionnaire);

        // Test encode
        const base64StringToUpload = encode(sourceJsonString);
        await TestObjectStorage.uploadObject(base64StringToUpload, 'test', 'application/json');
        const readBase64String = await TestObjectStorage.readObject('test');

        expect(base64StringToUpload).toEqual(readBase64String.message);
        const decodedJsonString = decode(readBase64String.message);
        expect(sourceJsonString).toEqual(decodedJsonString);
        const resultObject = JSON.parse(decodedJsonString);
        expect(resultObject).toBeDefined();
        expect(resultObject.resourceType).toEqual(vaildV4Questionnaire.resourceType);
    });
});

describe('SUCCESS CASES: Store registered resources on DDB and S3', () => {
    test('createResource', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`]);
        S3ObjectStorageService.uploadObject = jest.fn(TestObjectStorage.uploadObject);

        const tenantId = '1111';
        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);
        {
            // Store Patient
            const serviceResponse = await hybridDataService.createResource({
                resource: validV4Patient,
                resourceType: validV4Patient.resourceType,
                tenantId,
            });
            expect(serviceResponse.success).toBeTruthy();
            expect(TestObjectStorage.isEmpty()).toBeTruthy();
        }
        {
            // Store large Questionnaire
            const serviceResponse = await hybridDataService.createResource({
                resource: vaildV4Questionnaire,
                resourceType: vaildV4Questionnaire.resourceType,
                tenantId,
            });

            expect(serviceResponse.success).toBeTruthy();
            expect(TestObjectStorage.isEmpty()).toBeFalsy();
        }
    });

    test('readResource', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`]);
        S3ObjectStorageService.readObject = jest.fn(TestObjectStorage.readObject);

        const tenantId = '1111';

        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);
        {
            const resourceId = '123456';
            // Read Patient
            const serviceResponse = await hybridDataService.readResource({
                id: resourceId,
                resourceType: `Patient`,
                tenantId,
            });
            expect(serviceResponse.resource).toBeDefined();
            expect(serviceResponse.resource.resourceType).toEqual(`Patient`);
        }
        {
            await storeQuestionnaireBulkData(vaildV4Questionnaire, 'Test');

            const resourceId = '98765';
            // Read large Questionnaire
            const serviceResponse = await hybridDataService.readResource({
                id: resourceId,
                resourceType: `Questionnaire`,
                tenantId,
            });
            expect(serviceResponse.resource).toBeDefined();
            expect(serviceResponse.resource.resourceType).toEqual(`Questionnaire`);
            expect(serviceResponse.resource.item).toBeDefined();
        }
        {
            const resourceId = '123456';
            // Read Patient
            const serviceResponse = await hybridDataService.vReadResource({
                id: resourceId,
                vid: '1',
                resourceType: `Patient`,
                tenantId,
            });
            expect(serviceResponse.resource).toBeDefined();
            expect(serviceResponse.resource.resourceType).toEqual(`Patient`);
        }
        {
            await storeQuestionnaireBulkData(vaildV4Questionnaire, 'Test');
            const resourceId = '98765';
            // Read large Questionnaire
            const serviceResponse = await hybridDataService.vReadResource({
                id: resourceId,
                vid: '1',
                resourceType: `Questionnaire`,
                tenantId,
            });
            expect(serviceResponse.resource).toBeDefined();
            expect(serviceResponse.resource.resourceType).toEqual(`Questionnaire`);
            expect(serviceResponse.resource.item).toBeDefined();
        }
    });

    test('updateResource', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`]);
        S3ObjectStorageService.uploadObject = jest.fn(TestObjectStorage.uploadObject);

        const tenantId = '1111';
        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);
        {
            // Store large Questionnaire
            const serviceResponse = await hybridDataService.createResource({
                resource: vaildV4Questionnaire,
                resourceType: vaildV4Questionnaire.resourceType,
                tenantId,
            });

            expect(serviceResponse.success).toBeTruthy();
            expect(TestObjectStorage.numberOfObjects()).toEqual(1);

            // Update large Questionnaire
            const updateResponse = await hybridDataService.updateResource({
                id: serviceResponse.resource.id,
                resource: vaildV4Questionnaire,
                resourceType: vaildV4Questionnaire.resourceType,
                tenantId,
            });

            expect(updateResponse.success).toBeTruthy();
            expect(TestObjectStorage.numberOfObjects()).toEqual(2);
        }
    });

    test('deleteResource', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`]);
        S3ObjectStorageService.readObject = jest.fn(TestObjectStorage.readObject);
        S3ObjectStorageService.deleteObject = jest.fn(TestObjectStorage.deleteObject);
        const tenantId = '1111';

        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);
        {
            await TestObjectStorage.uploadObject(
                encode(JSON.stringify(vaildV4Questionnaire)),
                'Test',
                'application/json',
            );
            const resourceId = '98765';
            // Delete large Questionnaire
            const serviceResponse = await hybridDataService.deleteResource({
                id: resourceId,
                resourceType: `Questionnaire`,
                tenantId,
            });
            expect(serviceResponse.success).toBeTruthy();
            expect(TestObjectStorage.isEmpty()).toBeTruthy();
        }
        {
            const resourceId = '123456';
            // Delete Patient
            const serviceResponse = await hybridDataService.deleteResource({
                id: resourceId,
                resourceType: `Patient`,
                tenantId,
            });
            expect(serviceResponse.success).toBeTruthy();
        }
    });

    test('deleteResource multiple versions', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`, `Testv2`]);
        S3ObjectStorageService.readObject = jest.fn(TestObjectStorage.readObject);
        S3ObjectStorageService.deleteObject = jest.fn(TestObjectStorage.deleteObject);
        const tenantId = '1111';

        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);
        {
            await TestObjectStorage.uploadObject(
                encode(JSON.stringify(vaildV4Questionnaire)),
                'Test',
                'application/json',
            );
            await TestObjectStorage.uploadObject(
                encode(JSON.stringify(vaildV4Questionnaire)),
                'Testv2',
                'application/json',
            );
            const resourceId = '98765';
            // Delete large Questionnaire
            const serviceResponse = await hybridDataService.deleteResource({
                id: resourceId,
                resourceType: `Questionnaire`,
                tenantId,
            });
            expect(serviceResponse.success).toBeTruthy();
            expect(TestObjectStorage.isEmpty()).toBeTruthy();
        }
        {
            const resourceId = '123456';
            // Delete Patient
            const serviceResponse = await hybridDataService.deleteResource({
                id: resourceId,
                resourceType: `Patient`,
                tenantId,
            });
            expect(serviceResponse.success).toBeTruthy();
        }
    });
});

describe('ERROR CASES: Store registered resources on DDB and S3', () => {
    test('readResource: S3 object is not available', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`]);
        S3ObjectStorageService.readObject = jest.fn(TestObjectStorage.readObject);

        const tenantId = '1111';

        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);
        const resourceId = '98765';
        try {
            // Read large Questionnaire
            const serviceResponse = await hybridDataService.readResource({
                id: resourceId,
                resourceType: `Questionnaire`,
                tenantId,
            });
        } catch (e) {
            // CHECK
            expect(e).toEqual(new ResourceNotFoundError('Questionnaire', resourceId));
        }
    });

    test('createResource/updateResource: store on S3 failed', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`]);

        S3ObjectStorageService.uploadObject = jest.fn(TestObjectStorage.uploadObjectWithError);

        const tenantId = '1111';
        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);

        try {
            // Store large Questionnaire
            const serviceResponse = await hybridDataService.createResource({
                resource: vaildV4Questionnaire,
                resourceType: vaildV4Questionnaire.resourceType,
                tenantId,
            });
        } catch (e) {
            // CHECK
            expect(e).toEqual(new Error(`Failed uploading binary data to S3`));
        }
        try {
            const resourceId = `123456`;
            // Update large Questionnaire
            const updateResponse = await hybridDataService.updateResource({
                id: resourceId,
                resource: vaildV4Questionnaire,
                resourceType: vaildV4Questionnaire.resourceType,
                tenantId,
            });
        } catch (e) {
            // CHECK
            expect(e).toEqual(new Error(`Failed uploading binary data to S3`));
        }
    });

    test('deleteResource: deletion on ddb failed', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        mockDynamoDbDataService(dynamoDbDataService, [`Test`, `Testv2`]);

        // eslint-disable-next-line no-param-reassign
        dynamoDbDataService.deleteVersionedResource = jest.fn(async (id: string, vid: number, tenantId?: string) => {
            if (vid === 1) {
                throw new Error('ddb error aws error');
            }
            return {
                success: true,
                message: `Successfully deleted resource Id: ${id}, VersionId: ${vid}`,
            };
        });

        S3ObjectStorageService.readObject = jest.fn(TestObjectStorage.readObject);
        S3ObjectStorageService.deleteObject = jest.fn(TestObjectStorage.deleteObject);
        const tenantId = '1111';

        const hybridDataService = new HybridDataService(dynamoDbDataService, { enableMultiTenancy: true });
        hybridDataService.registerToStoreOnObjectStorage(`Questionnaire`, [`item`]);
        {
            await TestObjectStorage.uploadObject(
                encode(JSON.stringify(vaildV4Questionnaire)),
                'Test',
                'application/json',
            );
            await TestObjectStorage.uploadObject(
                encode(JSON.stringify(vaildV4Questionnaire)),
                'Testv2',
                'application/json',
            );
            const resourceId = '98765';
            // Delete large Questionnaire
            const serviceResponse = await hybridDataService.deleteResource({
                id: resourceId,
                resourceType: `Questionnaire`,
                tenantId,
            });
            expect(serviceResponse.success).toBeFalsy();
            expect(TestObjectStorage.numberOfObjects()).toEqual(1);
        }
        {
            const resourceId = '123456';
            // Delete Patient
            const serviceResponse = await hybridDataService.deleteResource({
                id: resourceId,
                resourceType: `Patient`,
                tenantId,
            });
            expect(serviceResponse.success).toBeFalsy();
        }
    });
});
