/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import { ExportJobStatus } from 'fhir-works-on-aws-interface';
import { pickBy } from 'lodash';
import {
    DynamoDBConverter,
    RESOURCE_TABLE,
    EXPORT_REQUEST_TABLE,
    EXPORT_REQUEST_TABLE_JOB_STATUS_INDEX,
} from './dynamoDb';
import { DynamoDbUtil, DOCUMENT_STATUS_FIELD, LOCK_END_TS_FIELD } from './dynamoDbUtil';
import DOCUMENT_STATUS from './documentStatus';
import { BulkExportJob } from '../bulkExport/types';

export default class DynamoDbParamBuilder {
    static LOCK_DURATION_IN_MS = 35 * 1000;

    static buildUpdateDocumentStatusParam(
        oldStatus: DOCUMENT_STATUS | null,
        newStatus: DOCUMENT_STATUS,
        rid: string,
        vid: number,
        resourceType: string,
        tenantId?: string,
    ) {
        let id = rid;
        const currentTs = Date.now();
        let futureEndTs = currentTs;
        if (newStatus === DOCUMENT_STATUS.LOCKED) {
            futureEndTs = currentTs + this.LOCK_DURATION_IN_MS;
        }
        if (tenantId !== undefined) {
            id += tenantId;
        }
        const params: any = {
            Update: {
                TableName: RESOURCE_TABLE,
                Key: DynamoDBConverter.marshall({
                    id,
                    vid,
                }),
                UpdateExpression: `set ${DOCUMENT_STATUS_FIELD} = :newStatus, ${LOCK_END_TS_FIELD} = :futureEndTs`,
                ExpressionAttributeValues: DynamoDBConverter.marshall({
                    ':newStatus': newStatus,
                    ':futureEndTs': futureEndTs,
                    ':resourceType': resourceType,
                }),
                ConditionExpression: `resourceType = :resourceType`,
            },
        };

        if (oldStatus) {
            params.Update.ConditionExpression = `resourceType = :resourceType AND (${DOCUMENT_STATUS_FIELD} = :oldStatus OR (${LOCK_END_TS_FIELD} < :currentTs AND (${DOCUMENT_STATUS_FIELD} = :lockStatus OR ${DOCUMENT_STATUS_FIELD} = :pendingStatus OR ${DOCUMENT_STATUS_FIELD} = :pendingDeleteStatus)))`;
            params.Update.ExpressionAttributeValues = DynamoDBConverter.marshall({
                ':newStatus': newStatus,
                ':oldStatus': oldStatus,
                ':lockStatus': DOCUMENT_STATUS.LOCKED,
                ':pendingStatus': DOCUMENT_STATUS.PENDING,
                ':pendingDeleteStatus': DOCUMENT_STATUS.PENDING_DELETE,
                ':currentTs': currentTs,
                ':futureEndTs': futureEndTs,
                ':resourceType': resourceType,
            });
        }

        return params;
    }

    static buildGetResourcesQueryParam(
        rid: string,
        resourceType: string,
        maxNumberOfVersions: number,
        projectionExpression?: string,
        tenantId?: string,
    ) {
        let id = rid;
        if (tenantId !== undefined) {
            id += tenantId;
        }

        const params: any = {
            TableName: RESOURCE_TABLE,
            ScanIndexForward: false,
            Limit: maxNumberOfVersions,
            FilterExpression: tenantId === undefined ? '#r = :resourceType' : '#r = :resourceType and #t = :tenantId',
            KeyConditionExpression: 'id = :hkey',
            ExpressionAttributeNames:
                tenantId === undefined ? { '#r': 'resourceType' } : { '#r': 'resourceType', '#t': 'tenantId' },
            ExpressionAttributeValues: DynamoDBConverter.marshall(
                pickBy(
                    {
                        ':hkey': id,
                        ':resourceType': resourceType,
                        ':tenantId': tenantId,
                    },
                    v => v !== undefined,
                ),
            ),
        };

        if (projectionExpression) {
            // @ts-ignore
            params.ProjectionExpression = projectionExpression;
        }
        return params;
    }

    static buildDeleteParam(rid: string, vid: number, tenantId?: string) {
        let id = rid;
        if (tenantId !== undefined) {
            id += tenantId;
        }
        const params: any = {
            Delete: {
                TableName: RESOURCE_TABLE,
                Key: DynamoDBConverter.marshall({
                    id,
                    vid,
                }),
            },
        };

        return params;
    }

    static buildGetItemParam(rid: string, vid: number, tenantId?: string) {
        const id = DynamoDbUtil.buildItemId(rid, tenantId);
        return {
            TableName: RESOURCE_TABLE,
            Key: DynamoDBConverter.marshall({
                id,
                vid,
            }),
        };
    }

    /**
     * Build DDB PUT param to insert a new resource
     * @param item - The object to be created and stored in DDB
     * @param allowOverwriteId - Allow overwriting a resource with the same id
     * @return DDB params for PUT operation
     */
    static buildPutAvailableItemParam(
        item: any,
        id: string,
        vid: number,
        allowOverwriteId: boolean = false,
        tenantId?: string,
    ) {
        const newItem = DynamoDbUtil.prepItemForDdbInsert(item, id, vid, DOCUMENT_STATUS.AVAILABLE, tenantId);
        const param: any = {
            TableName: RESOURCE_TABLE,
            Item: DynamoDBConverter.marshall(newItem),
        };

        if (!allowOverwriteId) {
            param.ConditionExpression = 'attribute_not_exists(id)';
        }
        return param;
    }

    static buildPutCreateExportRequest(bulkExportJob: BulkExportJob) {
        return {
            TableName: EXPORT_REQUEST_TABLE,
            Item: DynamoDBConverter.marshall(bulkExportJob),
        };
    }

    static buildQueryExportRequestJobStatus(jobStatus: ExportJobStatus, projectionExpression?: string) {
        const params = {
            TableName: EXPORT_REQUEST_TABLE,
            KeyConditionExpression: 'jobStatus = :hkey',
            ExpressionAttributeValues: DynamoDBConverter.marshall({
                ':hkey': jobStatus,
            }),
            IndexName: EXPORT_REQUEST_TABLE_JOB_STATUS_INDEX,
        };

        if (projectionExpression) {
            // @ts-ignore
            params.ProjectionExpression = projectionExpression;
        }

        return params;
    }

    static buildUpdateExportRequestJobStatus(jobId: string, jobStatus: ExportJobStatus) {
        const params = {
            TableName: EXPORT_REQUEST_TABLE,
            Key: DynamoDBConverter.marshall({
                jobId,
            }),
            UpdateExpression: 'set jobStatus = :newStatus',
            ConditionExpression: 'jobId = :jobIdVal',
            ExpressionAttributeValues: DynamoDBConverter.marshall({
                ':newStatus': jobStatus,
                ':jobIdVal': jobId,
            }),
        };

        return params;
    }

    static buildGetExportRequestJob(jobId: string) {
        const params = {
            TableName: EXPORT_REQUEST_TABLE,
            Key: DynamoDBConverter.marshall({
                jobId,
            }),
        };

        return params;
    }
}
