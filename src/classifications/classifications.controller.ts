import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { ClassificationsService } from './classifications.service.js';
import { StartClassificationDto } from './dtos/start-classification.dto.js';

@Controller('classifications')
export class ClassificationsController {
  constructor(
    private readonly classificationsService: ClassificationsService,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async startClassification(@Body() dto: StartClassificationDto) {
    return this.classificationsService.startClassification(dto);
  }

  @Get(':jobId')
  getJob(@Param('jobId') jobId: string) {
    return this.classificationsService.getJob(jobId);
  }

  @Get()
  listJobs(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    return this.classificationsService.listJobs(
      page ? parseInt(page, 10) : 1,
      limit ? parseInt(limit, 10) : 20,
    );
  }
}
