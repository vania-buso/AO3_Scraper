from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import re
import requests
from lxml import etree, html
import urllib.parse
import numpy as np

def ao3_metadata_by_page_to_csv(url, file_name, start_from = 1, n_pages = False):
    
    """Scrapes fanfictions metadata from the indicated url.
    Returns a .csv file with the metadata retrieved. By default it
    retrieves all pages available under specific search parameters, 
    starting from page 1. 
    
    Arguments:
        url (string): url string of the ao3 page with refined search parameters
        file_name (string): file name of the output .csv file. It must contain '.csv' as extension
        n_pages (optional, int): number of pages to be scraped. If it exceeds the pages available, 
            it retrieves all the available pages.
        start_from (optional, int): page number from which to start
    Output 
        metadata (.csv file): metadata retrieved with the scraper
    """
    
    headers = {"user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}    
    
    n_pages_url = requests.get(url)
    n_pages_html = n_pages_url.content
    n_pages_tree = html.fromstring(n_pages_html)
    if len(n_pages_html) <= 1:
        raise ValueError("--- No data retrieved ---")
    n_pages_list = n_pages_tree.xpath("//*[@id='main']/ol[2]/li/a/text()")
    n_pages_limit = int(n_pages_list[-2])
    
    if start_from != 1:
        if start_from > n_pages_limit:
            raise ValueError("'start_from' exceeds the page range for current search")
        else:
            start_from = int(start_from)
            if n_pages:
                if n_pages > n_pages_limit:
                    raise ValueError("'n_pages' over the page range for current search")
                else:
                    n_pages = int(n_pages)
            else:
                n_pages = n_pages_limit
    else:
        start_from = 1
        if n_pages:
            if n_pages > n_pages_limit:
                raise ValueError("'n_pages' over the page range for current search")
            else:
                n_pages = int(n_pages)
        else:
            n_pages = n_pages_limit
    
    print("Scraping {} pages out of {} pages available. Start at page: {}".format(n_pages, n_pages_limit, start_from))

    if re.search(r"page=", url) is None:
        parsed_url = url.split("?")
        for n in range(start_from, n_pages+1):
            page_url = "{}{}{}{}{}{}".format(parsed_url[0],"?","page=", str(n), "&", parsed_url[1])
            ao3_request = requests.get(page_url, headers = headers)
            ao3_html = ao3_request.text
            if len(ao3_html) <= 1:
                raise ValueError("--- No data retrieved! ---")
            ao3_soup = BeautifulSoup(ao3_html, "html.parser")
            md_dict = ao3_get_metadata(ao3_soup)
            md_df = ao3_df_metadata_from_dictionary(md_dict)
            clean_md_df = ao3_clean_df(md_df)
            if n == start_from:
                clean_md_df.to_csv(file_name, mode = "a", encoding = "utf-8", index = False)
                print("--- Page {} done! ---".format(n))
            else:
                clean_md_df.to_csv(file_name, mode = "a", encoding = "utf-8", index = False, header = False)
                print("--- Page {} done! ---".format(n))
            time.sleep(10)
        
        
    else:
        parsed_url = url.split('&', 2)
        page_fragment = parsed_url[1].split('=')
        for n in range(start_from, n_pages+1):
            page_url = "{}{}{}{}{}{}{}".format(parsed_url[0], '&', page_fragment[0], '=', str(n), '&', parsed_url[2])
            ao3_request = requests.get(page_url, headers = headers)
            ao3_html = ao3_request.text
            if len(ao3_html) <= 1:
                raise ValueError('--- No data retrieved! ---')
            ao3_soup = BeautifulSoup(ao3_html, 'html.parser')
            md_dict = ao3_get_metadata(ao3_soup)
            md_df = ao3_df_metadata_from_dictionary(md_dict)
            clean_md_df = ao3_clean_df(md_df)
            if n == start_from:
                clean_md_df.to_csv(file_name, mode = 'a', encoding = 'utf-8', index = False)
                print("--- Page {} done! ---".format(n))
            else:
                clean_md_df.to_csv(file_name, mode = 'a', encoding = 'utf-8', index = False, header = False)
                print("--- Page {} done! ---".format(n))
            time.sleep(10)
    return




def ao3_get_metadata(ao3_soup): 
    
    """Collects and organizes metadata from a BeautifulSoup object from an AO3 page. 
    Returns a metadata dictionary for the page.
    
    Arguments
        ao3_soup (BeautifulSoup object): AO3 page
    Output 
        metadata (dictionary): metadata of the 20 stories from AO3 page
    
    The dictionary contains the following keys with a list of 20 elements as associated values. 
    Some fields are not compulsory but left at author's discretion. 
    In some other cases, though the field might be mandatory, the author might decide to hide it from un-registered users.
    'Nan' indicates that for the story under the index position of 'Nan', the field was not found in the html.
    'optional/mandatory' describes whether the field is always found in the html or not.
    
    id: unique story identifier (mandatory)
    title: title of the story (optional)
    author: author of the story (optional)
    crossover_fandoms: fandoms with which the author identified a crossover (optional)
    ratings: rating of the story (mandatory)
    warnings: warning regarding possible major content triggers (mandatory)
    pairings: sexual orientations within the story (optional)
    status: 'Completed' or 'In-Progress' (mandatory)
    date_completion: date of the last installment published (mandatory)
    freeform_tags: unnormalized tags, i.e., descriptors at author's descretion, max. 50 items (optional)
    add_warnings: additional warnings (optional)
    characters: characters active in the story (optional)
    word_count: total number of words in the story (mandatory)
    n_chapters: number of chapters completed (optional)
    n_kudos: number of kudos left by readers (optional)
    n_comments: number of comments left by readers (optional)
    n_bookmarks: number of bookmarks left by readers (optional)
    n_hits: number of times the story's page has been visited (optional)
    languages: languages in which the story is written (mandatory)
    summary: the story's summary written by the author (optional)
    """
    
    metadata = {} 
    metadata["ids"] = [] 
    metadata["titles"] = []
    metadata["authors"] = []
    metadata["crossover_fandoms"] = []
    metadata["ratings"] = []
    metadata["warnings"] = []
    metadata["pairings"] = []
    metadata["completion_status"] = []
    metadata["dates_completion"] = []
    metadata["freeform_tags"] = []
    metadata["add_warnings"] = []
    metadata["relationships"] = []
    metadata["characters"] = []
    metadata["summaries"] = [] 
    metadata["word_counts"] = [] 
    metadata["n_chapters"] = [] 
    metadata["n_kudos"] = [] 
    metadata["n_comments"] = [] 
    metadata["n_bookmarks"] = [] 
    metadata["languages"] = [] 
    metadata["n_hits"] = [] 
    
    for story in ao3_soup.find_all("li", role = "article"): # for each story-item in the webpage, find:

        try: #title
            title = (story.find("h4", class_ = "heading").find("a")).text
        except:
            title = np.nan
        metadata["titles"].append(title)

        try: #id
            id_ = (story.find("h4", class_= "heading").find("a")).get("href")
        except:
            id_= np.nan
        metadata["ids"].append(id_.replace("/works/", ""))

        try: # author
            author = (story.find("a", rel = "author")).text
        except:
            author = np.nan
        metadata["authors"].append(author)

        story_crossovers = [] # crossover fandoms
        try:
            for fandom in story.find("h5", class_= "fandoms heading").find_all("a", class_= "tag"):
                story_crossovers.append(fandom.text)
        except:
            story_crossovers.append(np.nan)
        metadata["crossover_fandoms"].append(story_crossovers)

        try: #rating
            rating = (story.find("ul", class_= "required-tags").find_all("li")[0].find_all("span")[1]).text
        except:
            rating = np.nan
        metadata["ratings"].append(rating)

        try: #warnings
            warning = (story.find("ul", class_= "required-tags").find_all("li")[1].find_all("span")[1]).text
        except:
            warning = np.nan
        metadata["warnings"].append(warning)

        try: # pairing
            pairing = (story.find("ul", class_= "required-tags").find_all("li")[2].find_all("span")[1]).text
        except:
            pairing = np.nan
        metadata["pairings"].append(pairing)

        try: # completion 
            completion = (story.find("ul", class_= "required-tags").find_all("li")[3].find_all("span")[1]).text
        except:
            completion = np.nan
        metadata["completion_status"].append(completion)

        try: # date of completion (perhaps this can be automatically appended as datetime object)
            date_completion = story.find("p", class_ = "datetime").text
        except:
            date_completion = np.nan
        metadata["dates_completion"].append(date_completion)

        add_warning = [] #additional warnings (in freeform tags), put them in a list because I think there might be more than 1
        try:
            for a_warning in (story.find("li", class_ = "warnings")).find_all("a"):
                add_warning.append(a_warning.text)
        except:
            add_warning.append(np.nan)
        metadata["add_warnings"].append(add_warning)

        relationship = [] #relationships 
        try:
            for rel in (story.find('li', class_ = "relationships")).find_all("a"):
                relationship.append(rel.text)
        except:
            relationship.append(np.nan)
        metadata["relationships"].append(relationship)

        character = [] # characters
        try:
            for char in (story.find('li', class_ = "characters")).find_all("a"):
                character.append(char.text)
        except:
            character.append(np.nan)
        metadata["characters"].append(character)

        freeform = [] # freeform tags: max. is 50 per story
        try:
            for tag in (story.find("li", class_ = "freeforms")).find_all("a"):
                freeform.append(tag.text)
            for final_tag in (story.find("li", class_ = "freeforms last")).find_all("a"):
                freeform.append(final_tag.text)
        except:
            freeform.append(np.nan)
        if (len(freeform) > 1) and (np.isnan(freeform[-1])): # 'freeforms last' is interchangeable with 'freeforms' for last tag
            freeform.pop(-1)
        metadata["freeform_tags"].append(freeform)

        summary = [] # story summaries
        try:
            for summ in (story.find("blockquote", class_ = 'userstuff summary')).find_all("p"):
                summary.append(summ.text)
                story_summary = ' '.join(summary).replace("\n\n", "\n").strip() # this is just a first cleaning of the text. Not sure if keeping the white lines (there could be some) could help in the analysis of the layout, for example?
        except:
            story_summary = np.nan
        metadata["summaries"].append(story_summary)

        language = [] # languages - maybe there are stories with more than 1 language 
        try:
            for lan in (story.find("dl", class_ = "stats")).find_all("dd", class_= "language"): #must branch up to dl because the dd class ="language" is also a field of the scroll-down menu
                language.append(lan.text)
        except:
                language.append(np.nan)
        metadata["languages"].append(language)

        try: # word-count
            word_count = story.find("dd", class_ = "words")
            metadata["word_counts"].append(int(word_count.text.replace(",", "")))
        except:
            metadata["word_counts"].append(np.nan)

        try: # n_chapters
            n_chap = story.find("dd", class_ = "chapters")
            metadata["n_chapters"].append(int((n_chap.text.split('/'))[0])) # append only the actual n of chapters
        except:
            metadata["n_chapters"].append(np.nan)

        try: # n_kudos
            n_kudo = story.find("dd", class_= "kudos")
            metadata["n_kudos"].append(int(n_kudo.text.replace(",", "")))
        except:
            metadata["n_kudos"].append(np.nan)

        try: # n_comments
            n_comm = story.find("dd", class_ = "comments").find("a")
            metadata["n_comments"].append(int(n_comm.text.replace(",", "")))
        except:
            metadata["n_comments"].append(np.nan)

        try: # n_bookmarks
            n_book = story.find("dd", class_ = "bookmarks").find("a")
            metadata["n_bookmarks"].append(int(n_book.text.replace(",", "")))
        except:
            metadata["n_bookmarks"].append(np.nan)

        try: #n_hits
            n_h = story.find("dd", class_ = "hits")
            metadata["n_hits"].append(int(n_h.text.replace(",", "")))
        except:
            metadata["n_hits"].append(np.nan)
        
    return metadata

def ao3_metadata_from_page(url): 
    
    """Scrapes the metadata of a specific ao3 page. Returns a dictionary of metadata of the stories in the page.
    
    Arguments
        url(string): url string of AO3 page
    Output
        metadata dictionary(dictionary)
    """
    
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
    ao3_request = requests.get(url, headers = headers)
    ao3_html = ao3_request.text
    if len(ao3_html) < 1:
        raise ValueError('--- No data retrieved! ---')
    else:
        ao3_soup = BeautifulSoup(ao3_html, 'html.parser')
        return ao3_get_metadata(ao3_soup) 

def ao3_df_metadata_from_dictionary(md_dictionary): 
    
    """Constructs a pandas DataFrame object from a dictionary of metadata.
    
    Arguments
        md_dictionary (dictionary): AO3 metadata dictionary
    Output
        df_metadata (pandas DataFrame): DataFrame of AO3 metadata
    """
    
    df_metadata = pd.DataFrame.from_dict(md_dictionary)
    return df_metadata

def ao3_clean_df(ao3_df):
    
    """Cleans the metadata from an ao3 page by expanding list values. Returns a cleaned dataframe.
    
    Arguments
        ao3_df(pandas DataFrame): d instance of AO3 metadata
    Output
        clean_df (pandas DataFrame): clean DataFrame of AO3 metadata
        """
    clean_df = ao3_df.apply(lambda x: x.explode() if x.name in ['crossover_fandoms', 'characters', 'warnings', 'pairings', 'freeform_tags', 'add_warnings', 'relationships', 'languages'] else x) #can be simply done df.explode() with pandas 1.3.
    return clean_df

def ao3_metadata_by_page_to_df(url_list):
    
    """Given a list of urls of AO3 pages, returns a dataframe with the metadata of the stories for those pages.
    
    Arguments
        url_list(list): list of AO3 urls
    Output
        final_df(pandas DataFrame): dataframe instance containing metadata from AO3 pages specified in a list of urls 
    """
    
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
    pages_content = []
    for url in url_list:
        ao3_request = requests.get(url, headers = headers)
        ao3_html = ao3_request.text
        if len(ao3_html) <= 1:
            raise ValueError('--- No data retrieved! ---')
        ao3_soup = BeautifulSoup(ao3_html, 'html.parser')
        md_dict = ao3_get_metadata(ao3_soup)
        md_df = ao3_df_metadata_from_dictionary(md_dict)
        clean_md_df = ao3_clean_df(md_df) 
        pages_content.append(md_df)
        print("--- Page Done! ---")
        time.sleep(10)
    final_df = pd.concat(pages_content, ignore_index = True)
    return final_df

