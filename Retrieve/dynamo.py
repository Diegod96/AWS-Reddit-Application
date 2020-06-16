import boto3


def get_items():
    # Returns all the items from the article table
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("articles")
    return table
