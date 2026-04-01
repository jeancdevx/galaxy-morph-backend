import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { UploadsService } from './uploads.service.js';
import { RequestPresignedDto } from './dtos/request-presigned.dto.js';

@Controller('uploads')
export class UploadsController {
  constructor(private readonly uploadsService: UploadsService) {}

  @Post('presigned')
  @HttpCode(HttpStatus.OK)
  async getPresignedUrls(@Body() dto: RequestPresignedDto) {
    return this.uploadsService.generatePresignedUrls(dto);
  }
}
