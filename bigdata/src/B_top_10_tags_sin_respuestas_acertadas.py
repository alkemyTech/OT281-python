from functools import reduce
import re
from typing import Counter
import xml
import logging
import xml.etree.ElementTree as ET
import logging.config
from pathlib import Path


# Config logger
logging.config.fileConfig(
    Path(
        Path(__file__).parent.parent,
        'logs/B_logger.cfg'
    )
)

# Get logger
logger = logging.getLogger(__name__)


def chunkify(iterable, n_elements):
    """
    Splits data in chunks of n_elements

    @input: iterable, n_elements (int)
    
    @output: list of n_elements
    """
    for i in range(0, len(iterable), n_elements):
        yield iterable[i:i+n_elements]


def check_accepted_answers(data: xml.etree.ElementTree.Element) -> bool:
    """
    Gets a post(element tree object)
    and if the post contains an AcceptedAnswerId,
    this function will return true,
    otherwise, it will return false.

    @input: post (xml.etree.ElementTree.Element)

    @output: boolean
    """
    accepted_answer = None
    
    try:
        if data.attrib['AcceptedAnswerId']:
            accepted_answer =  True
    except Exception as ex:
        logger.error(ex)
        accepted_answer = False

    return accepted_answer


def get_tags(data: xml.etree.ElementTree.Element) -> str:
    """
    Gets a post(element tree object)
    and if the post has an AcceptedAnswerId, 
    tags will be returned.

    @input: post (xml.etree.ElementTree.Element)

    @output: str
    """
    tags = None

    # If the post doesn;t have accepted answers
    if not check_accepted_answers(data):
        try: 
            tags = data.attrib['Tags']
            tags = re.findall('<(.+?)>', tags)
        except Exception as ex:
            logger.error(ex)
    return tags


def mapper(data):
    tags = list(map(get_tags, data))
    tags = list(filter(None, tags))
    
    return tags


def flat_tags_list(x, y):
    """
    It takes the list that contains the lists of tags, 
    and flat that list.

    @input: tags: l

    @output: list
    """
    return x + y
    

def main():
    
    # Dataset Path
    path_file = Path(
        Path(__file__).parent.parent,
        'datasets/posts.xml'
    )

    # Read file
    tree = ET.parse(path_file)

    # Get root
    root = tree.getroot()
    
    # Split data in small parts
    data_chuncked = chunkify(root, 50)
    
    # Obtain tags
    tags = list(map(mapper, data_chuncked))
    tags = list(filter(lambda x: len(x) != 0, tags))
    
    # Since we have list in list in list, we will apply
    # flat_tags_list, twice 
    tags = list(reduce(flat_tags_list, tags))
    tags = list(reduce(flat_tags_list, tags))
    
    # Top 10 tags
    top_10_tags = Counter(tags).most_common(10)
    
    # Paths
    root_path = Path(__file__).parent.parent
    file_path = 'output/B_top_10_tags_sin_respuestas_acertadas.csv'

    # Save file
    with open(Path(root_path, file_path), 'w') as file:
        file.write('tags,appearances')
        for i in top_10_tags:
            file.write(f'\n{i[0]},{i[1]}')
    

if __name__ == '__main__':
    main()
