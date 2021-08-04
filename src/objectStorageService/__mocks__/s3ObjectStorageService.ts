/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

/* eslint-disable @typescript-eslint/no-unused-vars */
import ObjectStorageInterface from '../objectStorageInterface';

const ObjectStorageService: ObjectStorageInterface = class {
    static async uploadObject(base64Data: string, fileName: string, contentType: string) {
        return { success: true, message: '' };
    }

    static async readObject(fileName: string) {
        return { success: true, message: '' };
    }

    static async deleteObject(fileName: string) {
        return { success: true, message: '' };
    }

    static async getPresignedPutUrl(fileName: string) {
        const url = `https://VALID_S3_PUT_URL.com/${fileName}`;
        return { success: true, message: url };
    }

    static async getPresignedGetUrl(fileName: string) {
        const url = `https://VALID_S3_GET_URL.com/${fileName}`;
        return { success: true, message: url };
    }

    static async deleteBasedOnPrefix(fileName: string) {
        return { success: true, message: '' };
    }
};

export default ObjectStorageService;
