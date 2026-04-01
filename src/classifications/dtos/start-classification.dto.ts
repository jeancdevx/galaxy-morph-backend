import { Type } from 'class-transformer';
import {
  ArrayMaxSize,
  ArrayMinSize,
  IsArray,
  IsNotEmpty,
  IsString,
  ValidateNested,
} from 'class-validator';

export class ClassificationImageItem {
  @IsString()
  @IsNotEmpty()
  key: string;
}

export class StartClassificationDto {
  @IsString()
  @IsNotEmpty()
  clientId: string;

  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(50)
  @ValidateNested({ each: true })
  @Type(() => ClassificationImageItem)
  images: ClassificationImageItem[];
}
