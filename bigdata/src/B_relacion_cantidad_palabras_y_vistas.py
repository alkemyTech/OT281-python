import re
import xml
import logging
import logging.config
import xml.etree.ElementTree as ET
from pathlib import Path
from functools import reduce

# Root path   
root_path = Path(__file__).parent.parent 

# Config logger
logging.config.fileConfig(
    Path(
        root_path,
        'logs/B_logger.cfg'
    )
)

# Set up logger
logger = logging.getLogger(__name__)


def chunkify(iterable, n_elements):
    """
    Splits data in chunks of n_elements

    @input: iterable, n_elements (int)
    
    @output: list of n_elements
    """
    for i in range(0, len(iterable), n_elements):
        yield iterable[i:i+n_elements]


def get_post_nwords(post: xml.etree.ElementTree.Element) -> int:
    """
    It takes a post and extracts from it
    the body of the post, then counts the number of words 
    into the post's body.
    """
    # Get and clean body
    try:
        body = re.findall(
            '(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))',
            post.attrib['Body']
        )
    except Exception as ex:
        logger.error(ex)
        return None

    return len(body)


def get_post_views(post: xml.etree.ElementTree.Element) -> int:
    """
    Takes a post a extracts the number of views.
    """
    # Get post views
    try:
        views = int(post.attrib['ViewCount'])
    except Exception as ex:
        logger.error(ex)
        return None

    return views


def mapper(posts: list):
    body_words = list(map(get_post_nwords, posts))
    views = list(map(get_post_views, posts))

    return list(zip(body_words, views))


def flat_list(x:list, y:list) -> list:
    """
    It takes the list that contains the lists of tags, 
    and flat that list.

    @input: tags: l

    @output: list
    """
    return x + y


def main():
    # Read data 
    tree = ET.parse(
        Path(
            root_path,
            'datasets/posts.xml'
    ))
    root = tree.getroot()

    # Split data
    data_chuncked = chunkify(root, 50)
    
    # Get the number of words and views per post
    body_views = list(map(mapper, data_chuncked))

    # Since we have list in list in list, we will apply
    # flat_list, twice 
    flattened_body_views= list(reduce(flat_list, body_views))
    flattened_body_views= list(reduce(flat_list, body_views))
    
    # Save file
    save_file = 'output/B_relacion_cantidad_palabras_y_vistas.csv'

    with open(Path(root_path,save_file ), 'w') as file:
        # Headers    
        file.write('number_of_words,views')
        for i in flattened_body_views:
            # Records
            file.write(f'\n{i[0]},{i[1]}')


if __name__ == '__main__':
    main()
