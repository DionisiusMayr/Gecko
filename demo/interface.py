"""
This script generates the graphical interface used to interact with Geeko.
It uses the Streamlit library and basically interacts with Neo4j to get recommendations and statistics about the content in question.
"""
import time
import os

import numpy as np
import pandas as pd
import streamlit as st
from icrawler.builtin import GoogleImageCrawler
from graphdatascience import GraphDataScience

NEO4J_HOST = "neo4j://localhost"
GDS = GraphDataScience(NEO4J_HOST, aura_ds=False)
CONTENTS = ['anime', 'videogame', 'boardgame', 'movie']


def parse_keywords(keywords):
    return ', '.join(map(lambda x: str.capitalize(x), keywords))


def get_recommendation_raw(content_id, origin_kind, destination_kind, limit):
    result = GDS.run_cypher('''
        MATCH (c1:content {content_id:$CONTENT_ID, type:$ORIGIN_KIND})
        MATCH (c2:content {type:$DESTINATION_KIND})-[]->(k:keyword)
        WHERE (c1 <> c2)
        WITH c1, c2, k, gds.similarity.cosine(c1.embedding, c2.embedding) AS cosineSimilarity
        RETURN c2.type, c2.title, c2.content_id, c2.description, collect(k.keywords), cosineSimilarity
        ORDER BY cosineSimilarity DESC, c2.title
        LIMIT $LIMIT
        ''',
        params={
            'CONTENT_ID': content_id, 
            'ORIGIN_KIND': origin_kind, 
            'DESTINATION_KIND': destination_kind,
            'LIMIT': limit
        }
    )
    
    result = result.dropna()
    
    return [tuple(r) for r in result.values]


def get_id_from_text(text, kind):
    result = GDS.run_cypher(f'''
        MATCH (n:content {{type: "{kind}"}})
        WHERE toLower(n.title) CONTAINS toLower($text)
        RETURN n.content_id, n.type
        ''', params={'text': text}
    )
    result = result.dropna()
    return result.values[0]


def get_recommendation(text, kind):
    idd, origin_kind = get_id_from_text(text, kind)
    result = []
    for destination_kind in CONTENTS:
        result += get_recommendation_raw(idd, origin_kind, destination_kind, 2)
    return result


def get_recommendation_for(content_name, kind):
    """
    This function controls the recommendations being used for a given content.
    It provides a content_name and kind.
    """
    return get_recommendation(content_name, kind)


def get_image(kind, content_name, content_id):
    if not os.path.exists(f'./img/{kind}/{content_id}'):
        google_Crawler = GoogleImageCrawler(storage = {'root_dir': f'./img/{kind}/{content_id}/'})
        google_Crawler.crawl(keyword=f'{content_name} {kind} official image', max_num = 1)
    else:
        print(f'./img/{kind}/{content_id} already exists, not downloading.')


st.title('Geeko Universal Recommender System')

st.sidebar.image('./img/logo/geeko_logo.png')
st.sidebar.markdown("Welcome to Geeko, your universal recommender system - from Geeks, for Geeks!")

col1, col2 = st.columns(2)

with col1:
    content_name = st.text_input(label='Tell me what you like:')

with col2:
    kind = st.radio(
        "What kind of content is it?",
        # key="visibility",
        options=["Anime", "Boardgame", "Movie", "Videogame"],
    )

tab1, tab2, tab3 = st.tabs(["🦎 Recommendations", "📑 Content Data", "📊 Macro Statistics"])

with tab1:
    if st.button(f"Get recommendations!"):
        prog_bar = st.progress(0, 'Loading recommendations...')
    
        recs = get_recommendation_for(str.lower(content_name), str.lower(kind))
        st.write(f"Getting recommendations for the {kind} '{content_name}'...")
        
        img_load_state = st.text('Loading images...')
    
        for i, rec in enumerate(recs):
            try:
                with st.container():
                    col1, col2 = st.columns(2)

                    kind = rec[0]
                    content_name = rec[1]
                    content_id = rec[2]
                    content_desc = rec[3]
                    content_keywords = parse_keywords(rec[4])
                    content_similarity = rec[5]
            
            
                    caption = f"{str.capitalize(kind)}: {content_name}"
                    get_image(kind=kind, content_name=content_name, content_id=content_id)
                    img_path = f'./img/{kind}/{content_id}/{os.listdir(f"./img/{kind}/{content_id}")[0]}'
                    
                    with col1:
                        st.image(img_path, caption=caption, width=400)
                    
                    with col2:
                        st.write(f"**{str.capitalize(kind)}**: {content_name}")
                        st.write(content_desc)
                        st.write(f"**Similarity**: {content_similarity:.2f}")
                        st.write(f"**Keywords**: {content_keywords}")
                        
                    prog_bar.progress((i + 1) / len(recs), 'Loading recommendations...')
            except:
                print(content_name, kind, get_id_from_text(content_name, str.lower(kind))[0])
        
        img_load_state.text('Images loaded! ✅')

with tab2:
    if st.button(f"Get data!"):
        content_id, origin_kind = get_id_from_text(str.lower(content_name), str.lower(kind))
        limit = 1
        
        result = GDS.run_cypher(
            '''
            MATCH (c1:content {content_id:$CONTENT_ID, type:$ORIGIN_KIND})-[]->(k:keyword)
            WITH c1, k
            RETURN c1.type, c1.title, c1.content_id, c1.description, collect(k.keywords), c1.centrality, c1.pagerank
            LIMIT 1
            ''',
            params={
                'CONTENT_ID': content_id, 
                'ORIGIN_KIND': origin_kind, 
            }
        )
        try:
            col1, col2 = st.columns(2)
            caption = f"{str.capitalize(origin_kind)}: {content_name}"
            get_image(kind=str.lower(origin_kind), content_name=content_name, content_id=content_id)
            img_path = f'./img/{str.lower(origin_kind)}/{content_id}/{os.listdir(f"./img/{str.lower(origin_kind)}/{content_id}")[0]}'
            with col1:
                st.image(img_path, caption=caption, width=400)
            with col2:
                c_type, c_title, c_content_id, c_description, c_keywords, c_centrality, c_pagerank = result.values[0]
                
                st.write(f'**Description**: {c_description}')
                st.write(f'**Keywords**: {parse_keywords(c_keywords)}')
                st.write(f'**Centrality**: {c_centrality:.2f}')
                st.write(f'**Page Rank**: {c_pagerank:.2f}')
    
                result = GDS.run_cypher('''
                    MATCH (c1:content {content_id:$CONTENT_ID, type:$ORIGIN_KIND})-[:like]->(u:users)
                    RETURN size(collect(distinct u.user_id))
                    ''',
                    params={
                    'CONTENT_ID': content_id, 
                    'ORIGIN_KIND': origin_kind, 
                }
                )
                if result.values[0]:
                    c_total_users = result.values[0][0]
                else:
                    c_total_users = 0
    
                st.write(f'**Total users**: {c_total_users}')
        except:
            pass

with tab3:
    metric = st.radio(
        "Which metric?",
        options=["Page Rank", "Popularity"],
    )
    
    if st.button(f"Get statistics!"):
        if metric == 'Page Rank':
            result = GDS.run_cypher(
                '''
                MATCH (n1:content)
                WHERE n1.type = 'anime'
                RETURN n1.content_id, n1.type, n1.pagerank, n1.title, n1.description
                ORDER BY n1.pagerank DESC
                LIMIT 5
                UNION ALL
                MATCH (n1:content)
                WHERE n1.type = 'boardgame'
                RETURN n1.content_id, n1.type, n1.pagerank, n1.title, n1.description
                ORDER BY n1.pagerank DESC
                LIMIT 5
                UNION ALL 
                MATCH (n1:content)
                WHERE n1.type = 'movie'
                RETURN n1.content_id, n1.type, n1.pagerank, n1.title, n1.description
                ORDER BY n1.pagerank DESC
                LIMIT 5
                UNION ALL
                MATCH (n1:content)
                WHERE n1.type = 'videogame'
                RETURN n1.content_id, n1.type, n1.pagerank, n1.title, n1.description
                ORDER BY n1.pagerank DESC
                LIMIT 5
                '''
            )
            for res in result.values:
                try:
                    col1, col2 = st.columns(2)
                    
                    content_id, content_kind, content_pagerank, content_title, content_desc = res
                    
                    caption = f"{str.capitalize(content_kind)}: {content_title}"
                    get_image(kind=str.lower(content_kind), content_name=content_title, content_id=content_id)
                    img_path = f'./img/{str.lower(content_kind)}/{content_id}/{os.listdir(f"./img/{str.lower(content_kind)}/{content_id}")[0]}'
                    
                    with col1:
                        st.image(img_path, caption=caption, width=400)    
                    
                    with col2:
                        st.write(f'**{str.capitalize(content_kind)}**: {content_title}')
                        st.write(f'**Description**: {content_desc}')
                        st.write(f'**Page Rank**: {content_pagerank:.2f}')
                except:
                    pass
        if metric == 'Popularity':
            result = GDS.run_cypher(
                '''
                match (c:content {type:'anime'})-[:like]->(u:users)
                with c, u
                return c.title, c.content_id, c.type, size(collect(DISTINCT(u.user_id))) as total_users, c.description
                order by total_users DESC
                limit 5
                UNION ALL
                match (c:content {type:'boardgame'})-[:like]->(u:users)
                with c, u
                return c.title, c.content_id, c.type, size(collect(DISTINCT(u.user_id))) as total_users, c.description
                order by total_users DESC
                limit 5
                UNION ALL
                match (c:content {type:'videogame'})-[:like]->(u:users)
                with c, u
                return c.title, c.content_id, c.type, size(collect(DISTINCT(u.user_id))) as total_users, c.description
                order by total_users DESC
                limit 5
                UNION ALL
                match (c:content {type:'movie'})-[:like]->(u:users)
                with c, u
                return c.title, c.content_id, c.type, size(collect(DISTINCT(u.user_id))) as total_users, c.description
                order by total_users DESC
                limit 5
                '''
            )
            for res in result.values:
                try:
                    col1, col2 = st.columns(2)

                    content_title, content_id, content_kind, total_users, content_desc = res
                    caption = f"{str.capitalize(content_kind)}: {content_title}"
                    get_image(kind=str.lower(content_kind), content_name=content_title, content_id=content_id)
                    img_path = f'./img/{str.lower(content_kind)}/{content_id}/{os.listdir(f"./img/{str.lower(content_kind)}/{content_id}")[0]}'
                    
                    with col1:
                        st.image(img_path, caption=caption, width=400)    
                    
                    with col2:
                        st.write(f'**{str.capitalize(content_kind)}**: {content_title}')
                        st.write(f'**Description**: {content_desc}')
                        st.write(f'**Total users**: {total_users}')
                except:
                    pass