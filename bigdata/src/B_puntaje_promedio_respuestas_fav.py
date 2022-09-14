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


"""
Since the responses do not have the 'FavoriteCount' attribute, 
we will extract the 'FavoriteCount' value present in the parent
posts, then we will get n scores from an ordered list of scores 
taking into account the number of FavoriteCount.
"""


def get_id_favorite(post: xml.etree.ElementTree.Element) -> list:
    """
    Takes a post and returns its Id and FavoriteCount values
    if the post is a parent post and its favoriteCount values is higher than zero
    """
    # Initialize variables
    id = None
    favorite = None

    try:
        if int(post.attrib['PostTypeId']) == 1\
                and int(post.attrib['FavoriteCount']) != 0:

            id = int(post.attrib['Id'])
            favorite = int(post.attrib['FavoriteCount'])

    except Exception as ex:
        logger.error(ex)
        return None

    return [id, favorite]


def get_parent_n_score(post: xml.etree.ElementTree.Element) -> list:
    """
    Takes a post and returns its ParentId and score values
    if the post is an response post.
    """

    # Initialize variables
    parent_id = None
    score = None

    try:
        if int(post.attrib['PostTypeId']) == 2:
            parent_id = int(post.attrib['ParentId'])
            score = int(post.attrib['Score'])
    except Exception as ex:
        logger.error(ex)
        return None

    return [parent_id, score]


def mapper(posts):
    id_n_favorites = list(map(get_id_favorite, posts))
    parent_n_scores = list(map(get_parent_n_score, posts))

    # Filter
    id_n_favorites = list(filter(None, id_n_favorites))
    id_n_favorites = list(filter(lambda x: x[0] != None, id_n_favorites))
    id_n_favorites = list(filter(lambda x: len(x) != 0, id_n_favorites))

    parent_n_scores = list(filter(None, parent_n_scores))
    parent_n_scores = list(filter(lambda x: x[0] != None, parent_n_scores))
    parent_n_scores = list(filter(lambda x: len(x) != 0, parent_n_scores))

    return [id_n_favorites, parent_n_scores]


def flat_list(x: list, y: list) -> list:
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

    # Get the mapped data. Format = [id_n_favorites, parent_n_scores]
    mapped_data = list(map(mapper, data_chuncked))

    id_n_favorites = list(map(lambda x: x[0], mapped_data))
    parent_n_scores = list(map(lambda x: x[1], mapped_data))

    # Flat lists
    id_n_favorites = list(reduce(flat_list, id_n_favorites))
    parent_n_scores = list(reduce(flat_list, parent_n_scores))

    # Obtain ids and favorites separately
    ids = list(map(lambda x: x[0], id_n_favorites))
    favorites = list(map(lambda x: x[1], id_n_favorites))

    # Filtered responses
    filtered_responses = []

    for id, favorite in list(zip(ids, favorites)):
        # Filter by the id
        responses = list(filter(lambda x: x[0] == id, parent_n_scores))

        # Sort list
        responses = sorted(responses, key=lambda x: x[1], reverse=True)

        # Get n favorites
        responses = responses[0:favorite]

        # Add to a filtered response
        filtered_responses.append(responses)

    # Remove possible empty lists
    filtered_responses = list(
        filter(lambda x: len(x) != 0, filtered_responses))

    # Flat list
    filtered_responses = list(reduce(flat_list, filtered_responses))

    # Save file
    path_file = 'output/B_puntaje_promedio_respuestas_fav.csv'

    with open(Path(root_path, path_file), 'w') as file:
        file.write('parent_id,score')
        for response in filtered_responses:
            file.write(f'\n{response[0]},{response[1]}')


if __name__ == '__main__':
    main()
