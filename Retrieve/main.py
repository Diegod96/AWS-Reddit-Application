import re
from dynamo import get_items
import logging
import boto3
from botocore.exceptions import ClientError
from string import punctuation

articles = get_items()


def parse_table():
    # Parses the table into two lists, articles and comments
    articles_table = articles.scan()
    values_view = articles_table.values()
    value_iterator = iter(values_view)
    items = next(value_iterator)

    # Iterate through the items and assign the value of the comments and title key to their respected lists
    comments = []
    titles = []
    for item in items:
        comments.append(item.get("comments"))
        titles.append(item.get("title"))

    # Flatten the comments list
    comments = [val for sublist in comments for val in sublist]

    return comments, titles


def clean_comments(comments):
    # Cleans comments from any hyperlinks, weird special characters, emojis, etc.
    print("Cleaning comments...")
    clean_comments = []
    for comment in comments:
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002500-\U00002BEF"  # chinese char
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   u"\u2640-\u2642"
                                   u"\u2600-\u2B55"
                                   u"\u200d"
                                   u"\u23cf"
                                   u"\u23e9"
                                   u"\u231a"
                                   u"\ufe0f"  # dingbats
                                   u"\u3030"
                                   "]+", flags=re.UNICODE)

        comment = comment.replace("\n", "")
        comment = comment.replace("  ", "")
        comment = re.sub(r'^https?:\/\/.*[\r\n]*', '', comment, flags=re.MULTILINE)
        comment = ''.join(i for i in comment if not i in punctuation)
        comment = emoji_pattern.sub(r'', comment)
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        comment = _RE_COMBINE_WHITESPACE.sub(" ", comment).strip()

        clean_comments.append(comment)

    return clean_comments


def clean_titles(titles):
    # Cleans titles from any hyperlinks, weird special characters, emojis, etc.
    print("Cleaning titles...")
    clean_titles = []
    for title in titles:
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002500-\U00002BEF"  # chinese char
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   u"\u2640-\u2642"
                                   u"\u2600-\u2B55"
                                   u"\u200d"
                                   u"\u23cf"
                                   u"\u23e9"
                                   u"\u231a"
                                   u"\ufe0f"  # dingbats
                                   u"\u3030"
                                   "]+", flags=re.UNICODE)

        title = title.replace("\n", "")
        title = title.replace("  ", "")
        title = re.sub(r'^https?:\/\/.*[\r\n]*', '', title, flags=re.MULTILINE)
        title = ''.join(i for i in title if not i in punctuation)
        title = emoji_pattern.sub(r'', title)
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        title = _RE_COMBINE_WHITESPACE.sub(" ", title).strip()

        clean_titles.append(title)

    return clean_titles


def convert_and_push(comments_text_blob, titles_text_blob):
    # Put these two text blobs into the S3 bucket

    # Sending comments text blob to S3
    comments = '\n'.join(map(str, comments_text_blob))
    print("Sending comments text blob to S3 bucket...")
    s3 = boto3.resource("s3")
    bucket_name = "comments-and-titles"
    key = "comments/comments.txt"
    object = s3.Object(bucket_name, key)
    object.put(Body=comments)
    print("Comments text blob successfully sent to S3 bucket!")


    # Sending titles text blob to S3
    titles = '\n'.join(map(str, titles_text_blob))
    print("Sending titles text blob to S3 bucket...")
    s3 = boto3.resource("s3")
    bucket_name = "comments-and-titles"
    key = "titles/titles.txt"
    object = s3.Object(bucket_name, key)
    object.put(Body=titles)
    print("Titles text blob successfully sent to S3 bucket!")




if __name__ == '__main__':
    print("Retrieving comments...")
    comments = parse_table()[0]
    print("Retrieved comments!")

    print("Retrieving titles...")
    titles = parse_table()[1]
    print("Retrieved titles!")

    convert_and_push(clean_comments(comments), clean_titles(titles))
