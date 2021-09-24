/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

// eslint-disable-next-line import/no-extraneous-dependencies
import * as AWSMock from 'aws-sdk-mock';

import { GetItemInput, PutItemInput, QueryInput, UpdateItemInput } from 'aws-sdk/clients/dynamodb';
import AWS from 'aws-sdk';
import isEqual from 'lodash/isEqual';
// eslint-disable-next-line import/no-extraneous-dependencies
import {
    CreateResourceRequest,
    ReadResourceRequest,
    BundleResponse,
    InitiateExportRequest,
    ResourceNotFoundError,
    ExportJobStatus,
    ResourceVersionNotFoundError,
    InvalidResourceError,
    isResourceNotFoundError,
    isInvalidResourceError,
} from 'fhir-works-on-aws-interface';
import { TooManyConcurrentExportRequestsError } from 'fhir-works-on-aws-interface/lib/errors/TooManyConcurrentExportRequestsError';
import each from 'jest-each';
import { utcTimeRegExp, uuidRegExp } from '../../testUtilities/regExpressions';
import { DynamoDbBundleService } from './dynamoDbBundleService';
import { DynamoDbDataService } from './dynamoDbDataService';
import { DynamoDBConverter } from './dynamoDb';
import DynamoDbHelper from './dynamoDbHelper';
import DynamoDbParamBuilder from './dynamoDbParamBuilder';
import { ConditionalCheckFailedExceptionMock } from '../../testUtilities/ConditionalCheckFailedException';

// eslint-disable-next-line import/order
import sinon = require('sinon');

jest.mock('../bulkExport/bulkExport');
AWSMock.setSDKInstance(AWS);

beforeEach(() => {
    expect.hasAssertions();
});
afterEach(() => {
    AWSMock.restore();
});

describe('CREATE', () => {
    afterEach(() => {
        AWSMock.restore();
    });
    // BUILD
    const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
    const tenantId = '111111-aaaa-2222-bbb-33333344444';
    const resourceType = 'Patient';
    const resource = {
        id,
        resourceType,
        name: [
            {
                family: 'Jameson',
                given: ['Matt'],
            },
        ],
    };

    const createResourceHelper = async (request: CreateResourceRequest) => {
        // READ items (Success)
        AWSMock.mock('DynamoDB', 'putItem', (params: PutItemInput, callback: Function) => {
            callback(null, 'success');
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const serviceResponse = await dynamoDbDataService.createResource(request);

        // CHECK
        const expectedResource: any = { ...request.resource };
        expectedResource.meta = {
            ...expectedResource.meta,
            versionId: '1',
            lastUpdated: expect.stringMatching(utcTimeRegExp),
        };
        expectedResource.id = expect.stringMatching(uuidRegExp);

        expect(serviceResponse.success).toEqual(true);
        expect(serviceResponse.message).toEqual('Resource created');
        expect(serviceResponse.resource).toStrictEqual(expectedResource);
    };

    test('SUCCESS: Create Resource without meta', async () => {
        await createResourceHelper({ resource, resourceType });
    });
    test('SUCCESS: Create Resource with meta', async () => {
        const resourceWithMeta = {
            ...resource,
            meta: {
                versionId: 'shouldBeOverwritten',
                lastUpdated: 'yesterday',
                security: { system: 'skynet' },
                tag: [{ display: 'test' }, { display: 'test1' }],
            },
        };
        await createResourceHelper({ resource: resourceWithMeta, resourceType });
    });
    test('FAILED: Resource with Id already exists', async () => {
        // READ items (Success)
        AWSMock.mock('DynamoDB', 'putItem', (params: PutItemInput, callback: Function) => {
            callback(new ConditionalCheckFailedExceptionMock(), {});
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE, CHECK
        await expect(dynamoDbDataService.createResource({ resource, resourceType })).rejects.toThrowError(
            new InvalidResourceError('Resource creation failed, id matches an existing resource'),
        );
    });
    test('SUCCESS: With tenantId: Create Resource without meta', async () => {
        await createResourceHelper({ resource, resourceType, tenantId });
    });
});

describe('READ', () => {
    // beforeEach(() => {
    //     // Ensures that for each test, we test the assertions in the catch block
    //     expect.hasAssertions();
    // });
    afterEach(() => {
        AWSMock.restore();
        sinon.restore();
    });
    const readResourceHelper = async (request: ReadResourceRequest, resource: any) => {
        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .returns(Promise.resolve({ message: 'Resource found', resource }));

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const serviceResponse = await dynamoDbDataService.readResource(request);

        // CHECK
        expect(serviceResponse.message).toEqual('Resource found');
        expect(serviceResponse.resource).toStrictEqual(resource);
    };
    test('SUCCESS: Get Resource', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const resourceType = 'Patient';
        const resource = {
            id,
            resourceType,
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            meta: { versionId: '1', lastUpdated: new Date().toISOString() },
        };
        await readResourceHelper({ resourceType, id }, resource);
    });
    test('SUCCESS: Get Versioned Resource', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = '5';
        const resourceType = 'Patient';
        const resource = {
            id,
            vid: parseInt(vid, 10),
            resourceType,
            documentStatus: 'shouldberemoved',
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            meta: { versionId: vid, lastUpdated: new Date().toISOString() },
        };

        // READ items (Success)
        AWSMock.mock('DynamoDB', 'getItem', (params: GetItemInput, callback: Function) => {
            callback(null, {
                Item: DynamoDBConverter.marshall(resource),
            });
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB({ apiVersion: '2012-08-10' }));

        // OPERATE
        const serviceResponse = await dynamoDbDataService.vReadResource({ resourceType, id, vid });

        // CHECK
        expect(serviceResponse.message).toEqual('Resource found');
        const expectedResource = { ...resource } as any;
        delete expectedResource.vid;
        delete expectedResource.documentStatus;
        expect(serviceResponse.resource).toStrictEqual(expectedResource);
    });

    test('ERROR: Get Versioned Resource: Unable to find resource', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = '5';
        const resourceType = 'Patient';

        // READ items (Success)
        AWSMock.mock('DynamoDB', 'getItem', (params: GetItemInput, callback: Function) => {
            callback(null, { Item: undefined });
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE, CHECK
        await expect(dynamoDbDataService.vReadResource({ resourceType, id, vid })).rejects.toThrowError(
            new ResourceVersionNotFoundError(resourceType, id, vid),
        );
    });

    test('ERROR: Get Versioned Resource: resourceType of request does not match resourceType retrieved', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = '5';
        const resourceType = 'Patient';

        // READ items (Success)
        AWSMock.mock('DynamoDB', 'getItem', (params: GetItemInput, callback: Function) => {
            callback(null, { Item: DynamoDBConverter.marshall({ id, vid, resourceType: 'Observation' }) });
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        await expect(dynamoDbDataService.vReadResource({ resourceType, id, vid })).rejects.toThrowError(
            new ResourceVersionNotFoundError(resourceType, id, vid),
        );
    });

    test('SUCCESS: With tenanId: Get Resource', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const tenantId = '111111-aaaa-2222-bbb-33333344444';

        const resourceType = 'Patient';
        const resource = {
            id,
            resourceType,
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            meta: { versionId: '1', lastUpdated: new Date().toISOString() },
        };
        await readResourceHelper({ resourceType, id, tenantId }, resource);
    });
});

describe('UPDATE', () => {
    afterEach(() => {
        AWSMock.restore();
        sinon.restore();
    });

    test('SUCCESS: Update Resource without existing metadata', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const resourcev1 = {
            id,
            vid: 1,
            resourceType: 'Patient',
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
        };

        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .returns(Promise.resolve({ message: 'Resource found', resource: resourcev1 }));

        const vid = 2;
        const lastModified = '2020-06-18T20:20:12.763Z';
        const batchReadWriteServiceResponse: BundleResponse = {
            success: true,
            message: '',
            batchReadWriteResponses: [
                {
                    id,
                    vid: vid.toString(),
                    resourceType: 'Patient',
                    operation: 'update',
                    resource: {
                        ...resourcev1,
                        meta: { versionId: vid.toString(), lastUpdated: lastModified, security: { system: 'gondor' } },
                    },
                    lastModified,
                },
            ],
        };

        sinon
            .stub(DynamoDbBundleService.prototype, 'transaction')
            .returns(Promise.resolve(batchReadWriteServiceResponse));

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const serviceResponse = await dynamoDbDataService.updateResource({
            resourceType: 'Patient',
            id,
            resource: { ...resourcev1, meta: { security: { system: 'gondor' } } },
        });

        // CHECK
        const expectedResource: any = { ...resourcev1 };
        expectedResource.meta = {
            versionId: vid.toString(),
            lastUpdated: expect.stringMatching(utcTimeRegExp),
            security: { system: 'gondor' },
        };

        expect(serviceResponse.success).toEqual(true);
        expect(serviceResponse.message).toEqual('Resource updated');
        expect(serviceResponse.resource).toStrictEqual(expectedResource);
    });

    test('SUCCESS: Update Resource with existing meta', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const resourcev1 = {
            id,
            vid: 1,
            resourceType: 'Patient',
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            meta: {
                versionId: '1',
                lastUpdated: 'yesterday',
                security: { system: 'skynet' },
                tag: [{ display: 'test' }, { display: 'test1' }],
            },
        };

        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .returns(Promise.resolve({ message: 'Resource found', resource: resourcev1 }));

        const vid = 2;
        const input = { ...resourcev1, meta: { security: { system: 'gondor' } } };
        const expectedReturnFromBundle = {
            ...input,
            meta: { versionId: vid.toString(), lastUpdated: new Date().toISOString(), security: { system: 'gondor' } },
        };
        const batchReadWriteServiceResponse: BundleResponse = {
            success: true,
            message: '',
            batchReadWriteResponses: [
                {
                    id,
                    vid: vid.toString(),
                    resourceType: 'Patient',
                    operation: 'update',
                    resource: expectedReturnFromBundle,
                    lastModified: '2020-06-18T20:20:12.763Z',
                },
            ],
        };

        sinon
            .stub(DynamoDbBundleService.prototype, 'transaction')
            .returns(Promise.resolve(batchReadWriteServiceResponse));

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const serviceResponse = await dynamoDbDataService.updateResource({
            resourceType: 'Patient',
            id,
            resource: input,
        });

        // CHECK
        const expectedResource: any = { ...expectedReturnFromBundle };
        expectedResource.meta.lastUpdated = expect.stringMatching(utcTimeRegExp);

        expect(serviceResponse.success).toEqual(true);
        expect(serviceResponse.message).toEqual('Resource updated');
        expect(serviceResponse.resource).toStrictEqual(expectedResource);
    });

    test('SUCCESS: With TenantId: Update Resource with existing meta', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const tenantId = '111111-aaaa-2222-bbb-33333344444';

        const resourcev1 = {
            id: id + tenantId,
            vid: 1,
            resourceType: 'Patient',
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            meta: {
                versionId: '1',
                lastUpdated: 'yesterday',
                security: { system: 'skynet' },
                tag: [{ display: 'test' }, { display: 'test1' }],
            },
            tenantId,
        };

        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .returns(Promise.resolve({ message: 'Resource found', resource: resourcev1 }));

        const vid = 2;
        const input = { ...resourcev1, meta: { security: { system: 'gondor' } } };
        const expectedReturnFromBundle = {
            ...input,
            meta: { versionId: vid.toString(), lastUpdated: new Date().toISOString(), security: { system: 'gondor' } },
        };
        const batchReadWriteServiceResponse: BundleResponse = {
            success: true,
            message: '',
            batchReadWriteResponses: [
                {
                    id,
                    vid: vid.toString(),
                    tenantId,
                    resourceType: 'Patient',
                    operation: 'update',
                    resource: expectedReturnFromBundle,
                    lastModified: '2020-06-18T20:20:12.763Z',
                },
            ],
        };

        sinon
            .stub(DynamoDbBundleService.prototype, 'transaction')
            .returns(Promise.resolve(batchReadWriteServiceResponse));

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const serviceResponse = await dynamoDbDataService.updateResource({
            resourceType: 'Patient',
            id,
            resource: input,
            tenantId,
        });

        // CHECK
        const expectedResource: any = { ...expectedReturnFromBundle };
        expectedResource.meta.lastUpdated = expect.stringMatching(utcTimeRegExp);

        expect(serviceResponse.success).toEqual(true);
        expect(serviceResponse.message).toEqual('Resource updated');
        expect(serviceResponse.resource).toStrictEqual(expectedResource);
    });

    test('ERROR: Update Resource not present in DynamoDB', async () => {
        // BUILD
        const id = 'd3847e9f-a551-47b0-b8d9-fcb7d324bc2b';
        const resource = {
            id,
            vid: 1,
            resourceType: 'Patient',
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
        };
        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .throws(new ResourceNotFoundError('Patient', id));
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        try {
            await dynamoDbDataService.updateResource({ resourceType: 'Patient', id, resource });
        } catch (e) {
            // CHECK
            expect(isResourceNotFoundError(e)).toEqual(true);
            expect(e.message).toEqual(`Resource Patient/${id} is not known`);
        }
    });

    test('SUCCESS: Update Resource as Create', async () => {
        // BUILD
        const id = 'e264efb1-147e-43ac-92ea-a050bc236ff3';
        const resourceType = 'Patient';
        const resource = {
            resourceType,
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
        };
        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .throws(new ResourceNotFoundError('Patient', id));
        AWSMock.mock('DynamoDB', 'putItem', (params: PutItemInput, callback: Function) => {
            callback(null, 'success');
        });
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB(), true);

        // OPERATE
        const serviceResponse = await dynamoDbDataService.updateResource({ resourceType: 'Patient', id, resource });

        // CHECK
        const expectedResource: any = { ...resource };
        expectedResource.meta = {
            versionId: '1',
            lastUpdated: expect.stringMatching(utcTimeRegExp),
        };
        expectedResource.id = id;

        expect(serviceResponse.success).toEqual(true);
        expect(serviceResponse.message).toEqual('Resource created');
        expect(serviceResponse.resource).toStrictEqual(expectedResource);
    });

    test('ERROR: Id supplied for Update as Create is not valid', async () => {
        // BUILD
        const id = 'uuid:$deadbeef';
        const resourceType = 'Patient';
        const resource = {
            resourceType,
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
        };
        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .throws(new ResourceNotFoundError('Patient', id));
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB(), true);
        // OPERATE
        try {
            await dynamoDbDataService.updateResource({ resourceType: 'Patient', id, resource });
        } catch (e) {
            // CHECK
            expect(isInvalidResourceError(e)).toEqual(true);
            expect(e.message).toEqual(`Resource creation failed, id ${id} is not valid`);
        }
    });
});

describe('DELETE', () => {
    afterEach(() => {
        AWSMock.restore();
        sinon.restore();
    });

    test('Successfully delete resource', async () => {
        // BUILD
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const resourceType = 'Patient';
        const vid = 1;
        const resource = {
            id,
            vid,
            resourceType,
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            meta: { versionId: vid.toString(), lastUpdated: new Date().toISOString() },
        };

        // READ items (Success)
        AWSMock.mock('DynamoDB', 'query', (params: QueryInput, callback: Function) => {
            callback(null, {
                Items: [DynamoDBConverter.marshall(resource)],
            });
        });

        // UPDATE (delete) item (Success)
        AWSMock.mock('DynamoDB', 'updateItem', (params: UpdateItemInput, callback: Function) => {
            callback(null, {
                Items: [DynamoDBConverter.marshall(resource)],
            });
        });

        sinon
            .stub(DynamoDbHelper.prototype, 'getMostRecentUserReadableResource')
            .returns(Promise.resolve({ message: 'Resource found', resource }));

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const serviceResponse = await dynamoDbDataService.deleteResource({ resourceType, id });

        // CHECK
        expect(serviceResponse.success).toEqual(true);
        expect(serviceResponse.message).toEqual(
            `Successfully deleted ResourceType: ${resourceType}, Id: ${id}, VersionId: ${vid}`,
        );
    });
});

describe('updateCreateSupported flag', () => {
    test('defaults to false', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());
        expect(dynamoDbDataService.updateCreateSupported).toEqual(false);
    });
    test('retains value set at Persistence component creation', async () => {
        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB(), false);
        expect(dynamoDbDataService.updateCreateSupported).toEqual(false);
        const dynamoDbDataServiceWithUpdateCreate = new DynamoDbDataService(new AWS.DynamoDB(), true);
        expect(dynamoDbDataServiceWithUpdateCreate.updateCreateSupported).toEqual(true);
    });
});

describe('initiateExport', () => {
    const initiateExportRequest: InitiateExportRequest = {
        requesterUserId: 'userId-1',
        exportType: 'system',
        transactionTime: '2020-09-01T12:00:00Z',
        outputFormat: 'ndjson',
        since: '2020-08-01T12:00:00Z',
        type: 'Patient',
        groupId: '1',
    };

    test('Successful initiate export request', async () => {
        // BUILD
        // Return an export request that is in-progress
        AWSMock.mock('DynamoDB', 'query', (params: QueryInput, callback: Function) => {
            if (isEqual(params, DynamoDbParamBuilder.buildQueryExportRequestJobStatus('in-progress'))) {
                callback(null, {
                    Items: [DynamoDBConverter.marshall({ jobOwnerId: 'userId-2', jobStatus: 'in-progress' })],
                });
            }
            callback(null, {});
        });

        AWSMock.mock('DynamoDB', 'putItem', (params: QueryInput, callback: Function) => {
            // Successfully update export-request table with request
            callback(null, {});
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const jobId = await dynamoDbDataService.initiateExport(initiateExportRequest);

        // CHECK
        expect(jobId).toBeDefined();
    });

    each(['in-progress', 'canceling']).test(
        'throttle limit exceeds MAXIMUM_CONCURRENT_REQUEST_PER_USER because user already has an %s request',
        async (jobStatus: ExportJobStatus) => {
            // BUILD
            // Return an export request that is in-progress
            AWSMock.mock('DynamoDB', 'query', (params: QueryInput, callback: Function) => {
                if (
                    isEqual(
                        params,
                        DynamoDbParamBuilder.buildQueryExportRequestJobStatus(jobStatus, 'jobOwnerId, jobStatus'),
                    )
                ) {
                    callback(null, {
                        Items: [DynamoDBConverter.marshall({ jobOwnerId: 'userId-1', jobStatus })],
                    });
                }
                callback(null, {});
            });

            const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

            // OPERATE
            try {
                await dynamoDbDataService.initiateExport(initiateExportRequest);
            } catch (e) {
                // CHECK
                expect(e).toMatchObject(new TooManyConcurrentExportRequestsError());
            }
        },
    );

    test('throttle limit exceeded MAXIMUM_SYSTEM_LEVEL_CONCURRENT_REQUESTS because system already has a job in the "in-progress" status and the "canceling" status', async () => {
        // BUILD
        // Return two export requests that are in-progress
        AWSMock.mock('DynamoDB', 'query', (params: QueryInput, callback: Function) => {
            if (
                isEqual(
                    params,
                    DynamoDbParamBuilder.buildQueryExportRequestJobStatus('in-progress', 'jobOwnerId, jobStatus'),
                )
            ) {
                callback(null, {
                    Items: [DynamoDBConverter.marshall({ jobOwnerId: 'userId-2', jobStatus: 'in-progress' })],
                });
            } else if (
                isEqual(
                    params,
                    DynamoDbParamBuilder.buildQueryExportRequestJobStatus('canceling', 'jobOwnerId, jobStatus'),
                )
            ) {
                callback(null, {
                    Items: [DynamoDBConverter.marshall({ jobOwnerId: 'userId-3', jobStatus: 'canceling' })],
                });
            }
            callback(null, {});
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        try {
            await dynamoDbDataService.initiateExport(initiateExportRequest);
        } catch (e) {
            // CHECK
            expect(e).toMatchObject(new TooManyConcurrentExportRequestsError());
        }
    });
});

describe('cancelExport', () => {
    test('Successfully cancel job', async () => {
        // BUILD
        AWSMock.mock('DynamoDB', 'getItem', (params: QueryInput, callback: Function) => {
            callback(null, {
                Item: DynamoDBConverter.marshall({ requesterUserId: 'userId-1', jobStatus: 'in-progress' }),
            });
        });

        const updateJobSpy = sinon.spy();
        AWSMock.mock('DynamoDB', 'updateItem', (params: QueryInput, callback: Function) => {
            updateJobSpy(params);
            callback(null, {});
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        const jobId = '2a937fe2-8bb1-442b-b9be-434c94f30e15';
        // OPERATE
        await dynamoDbDataService.cancelExport(jobId);

        // CHECK
        expect(updateJobSpy.getCall(0).args[0]).toMatchObject(
            DynamoDbParamBuilder.buildUpdateExportRequestJobStatus(jobId, 'canceling'),
        );
    });

    each(['failed', 'completed']).test(
        'Job cannot be canceled because job is in an invalid state',
        async (jobStatus: ExportJobStatus) => {
            // BUILD
            AWSMock.mock('DynamoDB', 'getItem', (params: QueryInput, callback: Function) => {
                callback(null, {
                    Item: DynamoDBConverter.marshall({ requesterUserId: 'userId-1', jobStatus }),
                });
            });

            const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

            const jobId = '2a937fe2-8bb1-442b-b9be-434c94f30e15';
            // OPERATE
            try {
                await dynamoDbDataService.cancelExport(jobId);
            } catch (e) {
                // CHECK
                expect(e).toMatchObject(
                    new Error(`Job cannot be canceled because job is already in ${jobStatus} state`),
                );
            }
        },
    );
});

describe('getExportStatus', () => {
    test('Successfully get export job status', async () => {
        // BUILD
        AWSMock.mock('DynamoDB', 'getItem', (params: QueryInput, callback: Function) => {
            callback(null, {
                Item: DynamoDBConverter.marshall({
                    jobFailedMessage: '',
                    outputFormat: 'ndjson',
                    exportType: 'system',
                    transactionTime: '2020-09-13T17:19:21.475Z',
                    since: '2020-09-02T05:00:00.000Z',
                    requesterUserId: 'userId-1',
                    groupId: '',
                    jobId: '2a937fe2-8bb1-442b-b9be-434c94f30e15',
                    jobStatus: 'in-progress',
                    stepFunctionExecutionArn: '',
                    type: 'Patient',
                }),
            });
        });

        const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

        // OPERATE
        const exportStatus = await dynamoDbDataService.getExportStatus('2a937fe2-8bb1-442b-b9be-434c94f30e15');

        // CHECK
        expect(exportStatus).toMatchObject({
            jobStatus: 'in-progress',
            exportedFileUrls: [],
            transactionTime: expect.stringMatching(utcTimeRegExp),
            exportType: 'system',
            outputFormat: 'ndjson',
            since: expect.stringMatching(utcTimeRegExp),
            type: 'Patient',
            groupId: '',
            errorArray: [],
            errorMessage: '',
        });
    });
});

each(['cancelExport', 'getExportStatus']).test('%s:Unable to find job', async (testMethod: string) => {
    // BUILD
    AWSMock.mock('DynamoDB', 'getItem', (params: QueryInput, callback: Function) => {
        callback(null, {});
    });

    const dynamoDbDataService = new DynamoDbDataService(new AWS.DynamoDB());

    const jobId = '2a937fe2-8bb1-442b-b9be-434c94f30e15';
    try {
        // OPERATE
        if (testMethod === 'cancelExport') {
            await dynamoDbDataService.cancelExport(jobId);
        } else {
            await dynamoDbDataService.getExportStatus(jobId);
        }
    } catch (e) {
        expect(e).toMatchObject(new ResourceNotFoundError('$export', jobId));
    }
});
