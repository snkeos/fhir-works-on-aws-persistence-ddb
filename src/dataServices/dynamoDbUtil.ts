/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import { clone, generateMeta } from 'fhir-works-on-aws-interface';
import flatten from 'flat';
import { SEPARATOR } from '../constants';
import DOCUMENT_STATUS from './documentStatus';

export const DOCUMENT_STATUS_FIELD = 'documentStatus';
export const LOCK_END_TS_FIELD = 'lockEndTs';
export const VID_FIELD = 'vid';
export const TENANT_ID_FIELD = 'tenantId';
export const REFERENCES_FIELD = '_references';

export class DynamoDbUtil {
    static cleanItem(item: any, projectionExpression?: string) {
        const cleanedItem = clone(item);

        delete cleanedItem[DOCUMENT_STATUS_FIELD];
        delete cleanedItem[LOCK_END_TS_FIELD];
        delete cleanedItem[VID_FIELD];
        delete cleanedItem[REFERENCES_FIELD];
        if (DynamoDbUtil.hasTenantId(cleanedItem)) {
            DynamoDbUtil.cleanItemId(cleanedItem);
            // Usually the tenant id is removed during clean up
            // The only exeception is if it is explicitly requested by a projection expression,(e.g. processing transaction bundles)
            if (!(projectionExpression !== undefined && projectionExpression.search(TENANT_ID_FIELD) !== -1)) {
                delete cleanedItem[TENANT_ID_FIELD];
            }
        }
        // Return id instead of full id (this is only a concern in results from ES)
        const id = cleanedItem.id.split(SEPARATOR)[0];
        cleanedItem.id = id;

        return cleanedItem;
    }

    static hasTenantId(item: any) {
        return item[TENANT_ID_FIELD] !== undefined;
    }

    static cleanItemId(item: any) {
        if (item[TENANT_ID_FIELD] !== undefined) {
            const tenantIdStartIdx = item.id.indexOf(item[TENANT_ID_FIELD]);
            if (tenantIdStartIdx !== -1) {
                item.id = item.id.substring(0, tenantIdStartIdx); // eslint-disable-line no-param-reassign
            }
        }
    }

    static prepItemForDdbInsert(
        resource: any,
        id: string,
        vid: number,
        documentStatus: DOCUMENT_STATUS,
        tenantId?: string,
    ) {
        const item = clone(resource);
        if (tenantId !== undefined) {
            item.id = id + tenantId;
            item[TENANT_ID_FIELD] = tenantId;
        } else {
            item.id = id;
        }
        item.vid = vid;

        // versionId and lastUpdated for meta object should be system generated
        const { versionId, lastUpdated } = generateMeta(vid.toString());
        if (!item.meta) {
            item.meta = { versionId, lastUpdated };
        } else {
            item.meta = { ...item.meta, versionId, lastUpdated };
        }

        item[DOCUMENT_STATUS_FIELD] = documentStatus;
        item[LOCK_END_TS_FIELD] = Date.now();

        // Format of flattenedResource
        // https://www.npmjs.com/package/flat
        // flatten({ key1: { keyA: 'valueI' } })  => { key1.keyA: 'valueI'}
        const flattenedResources: Record<string, string> = flatten(resource);
        const references = Object.keys(flattenedResources)
            .filter((key: string) => {
                return key.endsWith('.reference');
            })
            .map((key: string) => {
                return flattenedResources[key];
            });
        item[REFERENCES_FIELD] = references;
        return item;
    }
}
