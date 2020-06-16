from praw.models import MoreComments

from config import reddit_instance
from dynamo import articles_table


articles = articles_table()
reddit = reddit_instance()



def subreddit_hot_submissions(sub="wallstreetbets"):
    # Connects to subreddit (sub)
    # Iterates through hot submissions and returns them in a list

    data = []
    subreddit = reddit.subreddit(sub)

    for submission in subreddit.hot():
        comments = []
        article = {
            "id": submission.id,
            "title": submission.title,
            "score": submission.score,
            "url": submission.url
        }

        for top_level_comment in submission.comments:
            if isinstance(top_level_comment, MoreComments):
                continue
            comments.append(top_level_comment.body)

        article["comments"] = comments
        data.append(article)
    return data





if __name__ == '__main__':

    data = subreddit_hot_submissions()
    for datum in data:
        print("Attempting to save article", datum["title"])
