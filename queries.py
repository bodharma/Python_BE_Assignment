"""
Followin part of the code is an graphql query.
Your main task here is to create isolated unit tests as well as can be reused for performance testing.

You should use pytest and should not have external connection.
You can use any type of mocks.
Follow unit testing best practices and if needed you are allowed to create more than one file and any structure.
To submit your version of solution make PR to main branch.
"""


import boto3
import strawberry


@strawberry.input
class GetProgramMediaInput:
    section_id: UUID
    content_type: str
    file_name: str

@strawberry.type
class MediaUploadUrlType:
    url: str
    fields: str



def get_s3_client(region_name="us-east-2") -> boto3.client:
    """Init and return s3 client."""
    return boto3.client(
        "s3",
        region_name=region_name,
        config=Config(
            s3={"addressing_style": "path"}, signature_version="s3v4"
        ),
    )


def generate_presigned_url(bucket_name: str, object_name: str, content_type: str, expiration=3600,
                           region_name="us-east-2"):
    response = get_s3_client(region_name).generate_presigned_post(
        Bucket=bucket_name,
        Key=object_name,
        Fields={"Content-Type": content_type},
        Conditions=[
            {"Content-Type": content_type}
        ],
        ExpiresIn=expiration
    )
    return response


async def get_media_upload_url(data: GetProgramMediaInput) -> MediaUploadUrlType:
    try:
        section = await ProgramModuleSectionRepository.get_program_module_section(
            data.section_id
        )
        if not section:
            raise Exception(f"Section with id {data.section_id} not found")
        module = await ProgramModuleRepository.get_program_module(
            section.program_module_id
        )
        if not module:
            raise Exception(f"Module with id {section.program_module_id} not found")
        program = await ProgramRepository.get_program(module.program_id)
        if not program:
            raise Exception(f"Program with id {module.program_id} not found")

        path2content_type = {
            "video/mp4": "video/src/",
            "video/quicktime": "video/src/",
            "application/pdf": "pdf/",
            "text/html": "html/",
        }
        object_name = f"programs/{program.title}/{path2content_type[data.content_type]}{data.file_name}"

        logger.info(
            f"Generating presigned url for {object_name} in bucket {settings.AWS_BUCKET_NAME}"
        )

        presigned_url = generate_presigned_url(
            bucket_name=settings.AWS_BUCKET_NAME,
            object_name=object_name,
            content_type=data.content_type,
            expiration=3600,
            region_name="us-east-2",
        )
        return MediaUploadUrlType(
            url=presigned_url["url"], fields=json.dumps(presigned_url["fields"])
        )
    except Exception as e:
        logger.exception(e)
        raise e
        
        
@strawberry.type()
class ProgramQuery:
    """Program graphql queries."""

    get_media_upload_url: MediaUploadUrlType = strawberry.field(
        resolver=get_media_upload_url
    )
        
