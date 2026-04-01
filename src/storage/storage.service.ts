import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { randomUUID } from 'node:crypto';

export interface PresignedUpload {
  key: string;
  putUrl: string;
  expiresAt: string;
}

@Injectable()
export class StorageService implements OnModuleInit {
  private readonly logger = new Logger(StorageService.name);
  private s3Client: S3Client;
  private bucketName: string;
  private presignedUrlExpiry: number;

  constructor(private readonly configService: ConfigService) {}

  onModuleInit() {
    const accountId = this.configService.get<string>('storage.accountId');
    const accessKeyId = this.configService.get<string>('storage.accessKeyId');
    const secretAccessKey = this.configService.get<string>(
      'storage.secretAccessKey',
    );
    const endpoint = this.configService.get<string>('storage.endpoint');

    this.bucketName = this.configService.get<string>('storage.bucketName')!;
    this.presignedUrlExpiry = this.configService.get<number>(
      'storage.presignedUrlExpiry',
    )!;

    this.s3Client = new S3Client({
      region: 'auto',
      endpoint,
      credentials: {
        accessKeyId: accessKeyId!,
        secretAccessKey: secretAccessKey!,
      },
    });

    this.logger.log(
      `R2 storage initialized (bucket: ${this.bucketName}, account: ${accountId})`,
    );
  }

  async generatePresignedPutUrl(
    filename: string,
    contentType: string,
  ): Promise<PresignedUpload> {
    const key = `galaxies/${randomUUID()}/${filename}`;

    const command = new PutObjectCommand({
      Bucket: this.bucketName,
      Key: key,
      ContentType: contentType,
    });

    const putUrl = await getSignedUrl(this.s3Client, command, {
      expiresIn: this.presignedUrlExpiry,
    });

    const expiresAt = new Date(
      Date.now() + this.presignedUrlExpiry * 1000,
    ).toISOString();

    this.logger.debug(`Generated presigned PUT URL for key: ${key}`);

    return { key, putUrl, expiresAt };
  }

  async generatePresignedGetUrl(key: string): Promise<string> {
    const command = new GetObjectCommand({
      Bucket: this.bucketName,
      Key: key,
    });

    return getSignedUrl(this.s3Client, command, {
      expiresIn: this.presignedUrlExpiry,
    });
  }

  getBucketName(): string {
    return this.bucketName;
  }
}
