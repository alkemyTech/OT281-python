from mrjob.job import MRJob
from mrjob.step import MRStep
import re

#main class
class MRmostViews(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.get_posts_views, 
                reducer=self.post_filter
            )
        ]

    #get posts ands views
    def get_posts_views(self, _, line):
       
        get_id = r'\sId="(.*?)"'
        get_views = r'\sViewCount="(.*?)"'
        id = re.search(get_id, line)
        views = re.search(get_views, line)
        # output <re.Match object; span=(4, 12), match=' Id="11"'>
        # output <re.Match object; span=(98, 115), match=' ViewCount="1836"'>

        #group() documentation: https://www.geeksforgeeks.org/re-matchobject-group-function-in-python-regex/
        if id:
            post_id = id.group(1)
        else:
            return ""
        if views:
            post_views = int(views.group(1).replace('"', ""))
        else:
            return 0
        # output "11"
        # output 1836

        yield None, {"id": post_id, "count": post_views}
    
    #get posts, sort them and return top 10
    def post_filter(self, _, line):
        posts = list(line)
        #sort posts
        posts = sorted(posts, key= lambda x: x.get("count"), reverse=True)[:10]
        #return top 10
        for i in posts:
            yield i.values()

if __name__ == "__main__":
    MRmostViews.run()
    