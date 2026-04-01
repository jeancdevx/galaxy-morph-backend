import { Injectable, Logger } from '@nestjs/common';
import {
  StorageService,
  PresignedUpload,
} from '../storage/storage.service.js';
import { RequestPresignedDto } from './dtos/request-presigned.dto.js';

@Injectable()
export class UploadsService {
  private readonly logger = new Logger(UploadsService.name);

  constructor(private readonly storageService: StorageService) {}

  async generatePresignedUrls(
    dto: RequestPresignedDto,
  ): Promise<{ uploads: PresignedUpload[] }> {
    this.logger.log(
      `Generating presigned URLs for ${dto.images.length} image(s)`,
    );

    const uploads = await Promise.all(
      dto.images.map((image) =>
        this.storageService.generatePresignedPutUrl(
          image.filename,
          image.contentType,
        ),
      ),
    );

    return { uploads };
  }
}
