import re
from dynamo import get_items
import boto3
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
    

    comments_text_blob = "".join(str(comment) for comment in comments)
    comments_text_blob = comments_text_blob.replace("\n", "")
    comments_text_blob = comments_text_blob.replace("  ", "")
    comments_text_blob = re.sub(r'^https?:\/\/.*[\r\n]*', '', comments_text_blob, flags=re.MULTILINE)
    comments_text_blob = ''.join(i for i in comments_text_blob if not i in punctuation)
    comments_text_blob = emoji_pattern.sub(r'', comments_text_blob)
    print("Comments cleaned!")

    return comments_text_blob



def clean_titles(titles):
    # Cleans titles from any hyperlinks, weird special characters, emojis, etc.
    print("Cleaning titles...")
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

    titles_text_blob = "".join(str(title) for title in titles)
    titles_text_blob = titles_text_blob.replace("\n", "")
    titles_text_blob = titles_text_blob.replace("  ", "")
    titles_text_blob = re.sub(r'^https?:\/\/.*[\r\n]*', '', titles_text_blob, flags=re.MULTILINE)
    titles_text_blob = ''.join(i for i in titles_text_blob if not i in punctuation)
    titles_text_blob = emoji_pattern.sub(r'', titles_text_blob)
    print("Titles cleaned!")

    return titles_text_blob


def convert_and_push(comments_text_blob, titles_text_blob):
    # Put these two text blobs into the S3 bucket

    # Sending comments text blob to S3
    print("Sending comments text blob to S3 bucket...")
    s3 = boto3.resource("s3")
    bucket_name = "comments-and-titles"
    key = "comments/comments.txt"
    object = s3.Object(bucket_name, key)
    object.put(Body=comments_text_blob)
    print("Comments text blob successfully sent to S3 bucket!")


    # Sending titles text blob to S3
    print("Sending titles text blob to S3 bucket...")
    s3 = boto3.resource("s3")
    bucket_name = "comments-and-titles"
    key = "titles/titles.txt"
    object = s3.Object(bucket_name, key)
    object.put(Body=titles_text_blob)
    print("Titles text blob successfully sent to S3 bucket!")





if __name__ == '__main__':
    
    print("Retrieving comments...")
    comments = parse_table()[0]
    print("Retrieved comments!")
    
    print("Retrieving titles...")
    titles = parse_table()[1]
    print("Retrieved titles!")
    

    convert_and_push(clean_comments(comments), clean_titles(titles))
    
    
    

