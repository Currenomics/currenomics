from dataclasses import dataclass


@dataclass(frozen=True)
class AwsCredentialInfo:
    aws_access_key: str
    aws_secret_access_key: str

    def get_dict(self) -> dict:
        return {
            "aws_access_key": self.aws_access_key,
            "aws_secret_access_key": self.aws_secret_access_key,
        }
