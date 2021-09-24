/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import uuidv4 from 'uuid/v4';
import {
    BatchReadWriteRequest,
    BatchReadWriteResponse,
    TypeOperation,
    SystemOperation,
} from 'fhir-works-on-aws-interface';
import { DynamoDbUtil } from './dynamoDbUtil';
import DOCUMENT_STATUS from './documentStatus';
import { DynamoDBConverter, RESOURCE_TABLE } from './dynamoDb';
import DynamoDbParamBuilder from './dynamoDbParamBuilder';

export interface ItemRequest {
    id: string;
    vid?: number;
    tenantId?: string;
    resourceType: string;
    operation: TypeOperation | SystemOperation;
    isOriginalUpdateItem?: boolean;
}

export default class DynamoDbBundleServiceHelper {
    static generateStagingRequests(requests: BatchReadWriteRequest[], idToVersionId: Record<string, number>) {
        const deleteRequests: any = [];
        const createRequests: any = [];
        const updateRequests: any = [];
        const readRequests: any = [];

        let newLocks: ItemRequest[] = [];
        let newBundleEntryResponses: BatchReadWriteResponse[] = [];

        requests.forEach(request => {
            switch (request.operation) {
                case 'create': {
                    // Add create request, put it in PENDING
                    let id = uuidv4();
                    if (request.id) {
                        id = request.id;
                    }

                    const vid = 1;
                    const Item = DynamoDbUtil.prepItemForDdbInsert(
                        request.resource,
                        id,
                        vid,
                        DOCUMENT_STATUS.PENDING,
                        request.tenantId,
                    );

                    createRequests.push({
                        Put: {
                            TableName: RESOURCE_TABLE,
                            Item: DynamoDBConverter.marshall(Item),
                        },
                    });
                    const { stagingResponse, itemLocked } = this.addStagingResponseAndItemsLocked(
                        request.operation,
                        {
                            ...request.resource,
                            meta: { ...Item.meta },
                            id,
                        },
                        request.tenantId,
                    );
                    newBundleEntryResponses = newBundleEntryResponses.concat(stagingResponse);
                    newLocks = newLocks.concat(itemLocked);
                    break;
                }
                case 'update': {
                    // Create new entry with status = PENDING
                    // When updating a resource, create a new Document for that resource
                    // if availabe id of the request always wins
                    let { id } = request;
                    if (id === undefined) {
                        id = request.resource.id;
                    }
                    if (id === undefined) {
                        id = uuidv4();
                        console.warn(`No id in order to update the resource => create a new id.`);
                    }
                    const vid = (idToVersionId[id] || 0) + 1;
                    const Item = DynamoDbUtil.prepItemForDdbInsert(
                        request.resource,
                        id,
                        vid,
                        DOCUMENT_STATUS.PENDING,
                        request.tenantId,
                    );

                    updateRequests.push({
                        Put: {
                            TableName: RESOURCE_TABLE,
                            Item: DynamoDBConverter.marshall(Item),
                        },
                    });

                    const { stagingResponse, itemLocked } = this.addStagingResponseAndItemsLocked(
                        request.operation,
                        {
                            ...request.resource,
                            meta: { ...Item.meta },
                        },
                        request.tenantId,
                    );
                    newBundleEntryResponses = newBundleEntryResponses.concat(stagingResponse);
                    newLocks = newLocks.concat(itemLocked);
                    break;
                }
                case 'delete': {
                    // Mark documentStatus as PENDING_DELETE
                    const { id, resourceType, tenantId } = request;
                    const vid = idToVersionId[id];
                    deleteRequests.push(
                        DynamoDbParamBuilder.buildUpdateDocumentStatusParam(
                            DOCUMENT_STATUS.LOCKED,
                            DOCUMENT_STATUS.PENDING_DELETE,
                            id,
                            vid,
                            resourceType,
                            tenantId,
                        ),
                    );
                    newBundleEntryResponses.push({
                        id,
                        vid: vid.toString(),
                        operation: request.operation,
                        lastModified: new Date().toISOString(),
                        resource: {},
                        resourceType: request.resourceType,
                    });
                    break;
                }
                case 'read': {
                    // Read the latest version with documentStatus = "LOCKED"
                    const { tenantId } = request;
                    const vid = idToVersionId[request.id];
                    const id = DynamoDbUtil.buildItemId(request.id, tenantId);
                    readRequests.push({
                        Get: {
                            TableName: RESOURCE_TABLE,
                            Key: DynamoDBConverter.marshall({
                                id,
                                vid,
                            }),
                        },
                    });
                    newBundleEntryResponses.push({
                        id,
                        vid: vid.toString(),
                        operation: request.operation,
                        lastModified: '',
                        resource: {},
                        resourceType: request.resourceType,
                    });
                    break;
                }
                default: {
                    break;
                }
            }
        });

        return {
            deleteRequests,
            createRequests,
            updateRequests,
            readRequests,
            newLocks,
            newStagingResponses: newBundleEntryResponses,
        };
    }

    static generateRollbackRequests(bundleEntryResponses: BatchReadWriteResponse[]) {
        let itemsToRemoveFromLock: { id: string; vid: string; resourceType: string; tenantId?: string }[] = [];
        let transactionRequests: any = [];
        bundleEntryResponses.forEach(stagingResponse => {
            switch (stagingResponse.operation) {
                case 'create':
                case 'update': {
                    /*
                        DELETE latest record
                        and remove lock entry from lockedItems
                     */
                    const {
                        transactionRequest,
                        itemToRemoveFromLock,
                    } = this.generateDeleteLatestRecordAndItemToRemoveFromLock(
                        stagingResponse.resourceType,
                        stagingResponse.id,
                        stagingResponse.vid,
                        stagingResponse.tenantId,
                    );
                    transactionRequests = transactionRequests.concat(transactionRequest);
                    itemsToRemoveFromLock = itemsToRemoveFromLock.concat(itemToRemoveFromLock);
                    break;
                }
                default: {
                    // For READ and DELETE we don't need to delete anything, because no new records were made for those
                    // requests
                    break;
                }
            }
        });

        return { transactionRequests, itemsToRemoveFromLock };
    }

    private static generateDeleteLatestRecordAndItemToRemoveFromLock(
        resourceType: string,
        id: string,
        vid: string,
        tenantId?: string,
    ) {
        const transactionRequest = DynamoDbParamBuilder.buildDeleteParam(id, parseInt(vid, 10), tenantId);
        const itemToRemoveFromLock = {
            id,
            vid,
            tenantId,
            resourceType,
        };

        return { transactionRequest, itemToRemoveFromLock };
    }

    static populateBundleEntryResponseWithReadResult(bundleEntryResponses: BatchReadWriteResponse[], readResult: any) {
        let index = 0;
        const updatedStagingResponses = bundleEntryResponses;
        for (let i = 0; i < bundleEntryResponses.length; i += 1) {
            const stagingResponse = bundleEntryResponses[i];
            // The first readResult will be the response to the first READ stagingResponse
            if (stagingResponse.operation === 'read') {
                let item = readResult?.Responses[index]?.Item;
                if (item === undefined) {
                    throw new Error('Failed to fulfill all READ requests');
                }
                item = DynamoDBConverter.unmarshall(item);
                item = DynamoDbUtil.cleanItem(item);

                stagingResponse.resource = item;
                stagingResponse.lastModified = item?.meta?.lastUpdated ? item.meta.lastUpdated : '';
                updatedStagingResponses[i] = stagingResponse;
                index += 1;
            }
        }
        return updatedStagingResponses;
    }

    private static addStagingResponseAndItemsLocked(operation: TypeOperation, resource: any, tenantId?: string) {
        const stagingResponse: BatchReadWriteResponse = {
            id: resource.id,
            vid: resource.meta.versionId,
            operation,
            lastModified: resource.meta.lastUpdated,
            resourceType: resource.resourceType,
            tenantId,
            resource,
        };
        const itemLocked: ItemRequest = {
            id: resource.id,
            vid: parseInt(resource.meta.versionId, 10),
            tenantId,
            resourceType: resource.resourceType,
            operation,
        };
        if (operation === 'update') {
            itemLocked.isOriginalUpdateItem = false;
        }

        return { stagingResponse, itemLocked };
    }
}
