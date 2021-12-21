/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import DynamoDB, { ItemList } from 'aws-sdk/clients/dynamodb';
import { GenericResponse, ResourceNotFoundError } from 'fhir-works-on-aws-interface';
import DynamoDbParamBuilder from './dynamoDbParamBuilder';
import { DynamoDBConverter } from './dynamoDb';
import DOCUMENT_STATUS from './documentStatus';
import { DOCUMENT_STATUS_FIELD, DynamoDbUtil } from './dynamoDbUtil';
const AWSXRay = require('aws-xray-sdk');

export default class DynamoDbHelper {
    private dynamoDb: DynamoDB;

    constructor(dynamoDb: DynamoDB) {
        this.dynamoDb = dynamoDb;
    }

    private async getMostRecentResources(
        resourceType: string,
        id: string,
        maxNumberOfVersionsToGet: number,
        projectionExpression?: string,
        tenantId?: string,
    ): Promise<ItemList> {
        const subsegment = AWSXRay.getSegment();
        const newSubseg = subsegment.addNewSubsegment(`getMostRecentResources.DynamoDbParamBuilder.buildGetResourcesQueryParam`);
        const params = DynamoDbParamBuilder.buildGetResourcesQueryParam(
            id,
            resourceType,
            maxNumberOfVersionsToGet,
            projectionExpression,
            tenantId,
        );
        newSubseg.close();

        const newSubseg2 = subsegment.addNewSubsegment(`getMostRecentResources.DynamoDb.query`);
        let result: any = {};
        try {
            result = await this.dynamoDb.query(params).promise();
        } catch (e) {
            if (e.code === 'ConditionalCheckFailedException') {
                throw new ResourceNotFoundError(resourceType, id);
            }
            throw e;
        }
        newSubseg2.close();

        const newSubseg3 = subsegment.addNewSubsegment(`getMostRecentResources.DynamoDBConverter.unmarshall(ddbJsonItem)`);
        const items = result.Items
            ? result.Items.map((ddbJsonItem: any) => DynamoDBConverter.unmarshall(ddbJsonItem))
            : [];
        if (items.length === 0) {
            newSubseg3.close();
            throw new ResourceNotFoundError(resourceType, id);
        }
        newSubseg3.close();
        return items;
    }

    async getMostRecentResource(
        resourceType: string,
        id: string,
        projectionExpression?: string,
        tenantId?: string,
    ): Promise<GenericResponse> {
        const subsegment = AWSXRay.getSegment();
        const newSubseg = subsegment.addNewSubsegment(`getMostRecentResource.getMostRecentResources`);
        let item = (await this.getMostRecentResources(resourceType, id, 1, projectionExpression, tenantId))[0];
        newSubseg.close();

        const newSubseg2 = subsegment.addNewSubsegment(`getMostRecentResource.clean`);
        item = DynamoDbUtil.cleanItem(item, projectionExpression);
        newSubseg2.close();
        return {
            message: 'Resource found',
            resource: item,
        };
    }

    /**
     * @return The most recent resource that has not been deleted and has been committed to the database (i.e. The resource is not in a transitional state)
     */
    async getMostRecentUserReadableResource(
        resourceType: string,
        id: string,
        tenantId?: string,
    ): Promise<GenericResponse> {
        const subsegment = AWSXRay.getSegment();
        const newSubseg = subsegment.addNewSubsegment(`getMostRecentUserReadableResource.getMostRecentResources`);
        const items = await this.getMostRecentResources(resourceType, id, 2, undefined, tenantId);
        newSubseg.close();

        const newSubseg2 = subsegment.addNewSubsegment(`getMostRecentUserReadableResource.select&clean`);
        const latestItemDocStatus: DOCUMENT_STATUS = <DOCUMENT_STATUS>items[0][DOCUMENT_STATUS_FIELD];
        if (latestItemDocStatus === DOCUMENT_STATUS.DELETED) {
            throw new ResourceNotFoundError(resourceType, id);
        }
        let item: any = {};
        // Latest version that are in LOCKED/PENDING_DELETE/AVAILABLE are valid to be read from
        if (
            [DOCUMENT_STATUS.AVAILABLE, DOCUMENT_STATUS.PENDING_DELETE, DOCUMENT_STATUS.LOCKED].includes(
                latestItemDocStatus,
            )
        ) {
            // eslint-disable-next-line prefer-destructuring
            item = items[0];
        } else if (latestItemDocStatus === DOCUMENT_STATUS.PENDING && items.length > 1) {
            // If the latest version of the resource is in PENDING, grab the previous version
            // eslint-disable-next-line prefer-destructuring
            item = items[1];
        } else {
            throw new ResourceNotFoundError(resourceType, id);
        }
        item = DynamoDbUtil.cleanItem(item);
        newSubseg2.close();
        return {
            message: 'Resource found',
            resource: item,
        };
    }
}
