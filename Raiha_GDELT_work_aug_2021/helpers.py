import pandas as pd
import plotly.graph_objects as go
import re
import redivis
import traceback


def search_gdelt_events_by_terms(term_to_query, relevant_context_terms=[], limit=0, after_year='',
                                 cols_to_query=['actor1name', 'actor2name', 'sourceurl']):
    """Generates SQL query and extracts GDELT events based on term_to_query from topic_cols.
    Searches for lowercase, uppercase, and capitalized versions of terms.
    :param term_to_query: List of terms or prefixes to search for.
    :param relevant_context_terms: List of terms we want to ensure are also in the entry, along with `term_to_query`.
    :param limit: Output limit for SQL query.
    :param after_year: Year up to and including which we want events from.
    :param cols_to_query: Columns from which terms will be queried. """

    # EXAMPLE OF QUERY
    # query = f"""
    #     SELECT globaleventid, actor1name, actor2name, monthyear, eventcode, quadclass, sourceurl
    #     FROM `cdpdemo.gdelt_demo.gdelt_events`
    #     WHERE (year >= 2010)
    #     AND (actor1name LIKE '%{word1.lower()}%' OR actor1name LIKE '%{word1.upper()}%' OR actor1name LIKE '%{word1.capitalize()}%'
    #         OR actor2name LIKE '%{word1.lower()}%' OR actor2name LIKE '%{word1.upper()}%' OR actor2name LIKE '%{word1.capitalize()}%'
    #         OR sourceurl LIKE '%{word1.lower()}%' OR sourceurl LIKE '%{word1.upper()}%' OR sourceurl LIKE '%{word1.capitalize()}%')
    #     AND (actor1name LIKE '%{word2.lower()}%' OR actor1name LIKE '%{word2.upper()}%' OR actor1name LIKE '%{word2.capitalize()}%'
    #         OR actor2name LIKE '%{word2.lower()}%' OR actor2name LIKE '%{word2.upper()}%' OR actor2name LIKE '%{word2.capitalize()}%'
    #         OR sourceurl LIKE '%{word2.lower()}%' OR sourceurl LIKE '%{word2.upper()}%' OR sourceurl LIKE '%{word2.capitalize()}%')
    #     LIMIT 10
    # """

    # Query below contains the columns selected to query
    query = """
        SELECT globaleventid, actor1code, actor1name, actor2code, 
        actor2name, monthyear, year, eventcode, quadclass, sourceurl
        FROM `cdpdemo.gdelt_demo.gdelt_events`
        WHERE """

    date_filter_text = after_year if after_year == "" else f" (year >= {str(after_year)}) \n"
    query += date_filter_text

    next_where_clause_start = "(" if after_year == "" else "AND ("
    query += next_where_clause_start

    # Must have each word in the full term (i.e. "contingent worker": must find "contingent" AND "worker" in any of the columns)
    term_to_query_split = term_to_query.split(' ')
    for w, word in enumerate(term_to_query_split):
        for c, col in enumerate(cols_to_query):
            stmt_add = f"{col} LIKE '%{word.lower()}%' OR {col} LIKE '%{word.upper()}%' OR {col} LIKE '%{word.capitalize()}%'"
            if c + 1 < len(cols_to_query):
                stmt_add += " OR "
            else:
                stmt_add += ") "
            query += stmt_add
        if w + 1 < len(term_to_query_split):
            query += "AND ("

    # May find any of the relevant conext terms in any of the columns
    if relevant_context_terms:
        query += 'AND ('
        for w, word in enumerate(relevant_context_terms):
            for c, col in enumerate(cols_to_query):
                stmt_add = f"{col} LIKE '%{word.lower()}%' OR {col} LIKE '%{word.upper()}%' OR {col} LIKE '%{word.capitalize()}%'"
                if c + 1 < len(cols_to_query):
                    stmt_add += " OR "
                query += stmt_add
            if w + 1 == len(relevant_context_terms):
                query += ") "
            else:
                query += " OR "

    if limit > 0:
        query += f'    LIMIT {str(limit)} \n'

    df_query = pd.DataFrame()
    try:
        df_query = redivis.query(query).to_dataframe()
    except:
        print(query)
        traceback.print_exc()

    return df_query, query


def store_event_info_for_terms(terms_to_query, type_of_term, cameo_mapping, limit=0, after_year=''):
    """Returns dictionary of information containing events data, event codes,
    actors, article URL and article date for each term.

    Example of terms_to_query = {
    'Diversity': {'terms': ['divers', 'inclusi'],
                  'relevant_context': [...]},
    'Discrimination': {'terms': ['discriminat'],
                       'relevant_context': [...]}}
    """

    dfs = {}
    actor_codes_found = []
    event_codes_found = []  # To store all unique event codes for heuristic (for viz)

    for term_type, terms_and_context in terms_to_query.items():
        terms = terms_and_context.get('terms')
        relevant_context = terms_and_context.get('relevant_context', [])

        for term in terms:
            term_df, term_query = search_gdelt_events_by_terms(
                term_to_query=term,
                relevant_context_terms=relevant_context,
                limit=limit, after_year=after_year)

            # Strip whitespace from actor code columns
            for col in ['actor1code', 'actor1name', 'actor2code', 'actor2name']:
                term_df[col] = term_df[col].str.strip()

            # 8/2: Drop duplicate articles based on sourceurl
            term_df = term_df.drop_duplicates(subset='sourceurl', keep="last")

            # 8/4: Add column for heuristic
            # Ex: type_of_term = 'DEI Heuristic' or 'Term type'
            # Ex: term_type = 'Diversity' or 'Risk'
            term_df[type_of_term] = term_type

            # 8/2: Get term actor codes
            term_actor_codes = []
            for code_col in ['actor1code', 'actor2code']:
                term_col_codes = term_df[code_col].unique()
                term_col_codes = [code for code in term_col_codes if str(code) != '<NA>']  # Remove NAs
                term_col_codes = list(set(term_col_codes))
                term_actor_codes = term_actor_codes + term_col_codes
            term_actor_codes = sorted(list(set(term_actor_codes)))
            actor_codes_found = actor_codes_found + term_actor_codes

            # Get term event codes
            term_event_codes = sorted(term_df['eventcode'].unique())
            event_codes_found = event_codes_found + term_event_codes

            # Add event code descriptions to term df
            term_df_w_event_code_desc = pd.merge(term_df, cameo_mapping,
                                                 left_on='eventcode', right_on='Event code', how='inner')
            term_df_w_event_code_desc.drop('eventcode', inplace=True, axis=1)  # drop duplicate column

            # Store term information in dict
            dfs[term] = {'df': term_df_w_event_code_desc, 'query': term_query,
                         'actor_codes': term_actor_codes, 'event_codes': term_event_codes}

    actor_codes_found = sorted(list(set(actor_codes_found)))
    event_codes_found = sorted(list(set(event_codes_found)))

    return dfs, actor_codes_found, event_codes_found


# Function to get outcome terms found in exposure term articles
def get_outcome_term_occurrences_in_exposure_articles(exposure_terms_dict, outcome_terms_lst,
                                                      exposure_terms_dfs,
                                                      exposure_type='Practice',
                                                      outcome_type='Risk', exposure_category_col='DEI Practice'):
    """Returns dictionaries of the following:
    1) Events with exposure terms with at least 1 outcome term, by exposure term {'discriminat': pd.DataFrame()}
    2) Events with exposure terms with at least 1 outcome term, by exposure term category/heuristic {'Diversity': pd.DataFrame()}
    3) Events with exposure terms with at least 1 outcome term, by exposure term and outcome term {'discriminat': {'boycott': pd.DataFrame()}}

    :param exposure_terms_dict: Categorized dictionary of exposure terms.
    :param outcome_terms_lst: List of outcome_terms. 
    :param exposure_terms_dfs: Output from function store_event_info_for_terms(). 
    :param exposure_type: The type of terms whose articles we want to look for outcome terms. Could be one of ['Practice', 'Risk', 'DEI Practice']. 
    :param outcome_type: The type of terms we are looking for in exposure term articles. Could be one of ['Risk', 'Practice', 'DEI Practice'].
    :param exposure_category_col: Category of type 1 terms. Could be one of ['Risk', 'Practice', 'DEI Practice']. """

    print(f'(For every {exposure_type} term, searching its articles for {outcome_type} terms).')

    # Using keys instead of exposure_terms from list, in case events from certain terms were combined after querying
    # e.g.: 'hiring' events combined under 'hire' events
    columns_df = exposure_terms_dfs[list(exposure_terms_dfs.keys())[0]]['df'].columns
    exposure_term_category_ALL_outcome_occurrences = {category: pd.DataFrame(columns=columns_df)
                                                      for category in exposure_terms_dict.keys()}
    exposure_term_ALL_outcome_occurrences = {exposure_term: pd.DataFrame(columns=columns_df)
                                             for exposure_term in exposure_terms_dfs.keys()}
    exposure_term_individual_outcome_occurrences = {exposure_term: {outcome_term: pd.DataFrame(columns=columns_df)
                                                                    for outcome_term in outcome_terms_lst}
                                                    for exposure_term in exposure_terms_dfs.keys()}
    for exposure_term in exposure_terms_dfs.keys():
        term_events_df = exposure_terms_dfs[exposure_term]['df']
        print(f'Searching "{exposure_term}" events (count: {term_events_df.shape[0]})...')

        # Check each "practice term event" for risk terms (e.g. exposure = practice, outcome = risk)
        for idx, event_row in term_events_df.iterrows():
            exposure_event_url = event_row['sourceurl']
            exposure_event_code_desc = event_row['Event code description']
            exposure_category = event_row[exposure_category_col]
            # print(exposure_category)

            for outcome_term in outcome_terms_lst:
                outcome_term_FOUND = False
                outcome_term_words = outcome_term.split(' ')

                if len(outcome_term_words) == 3:
                    outcome_term_word_count = 0
                    for word in outcome_term_words:
                        if (word in exposure_event_url) or (word in exposure_event_code_desc):
                            outcome_term_word_count += 1
                    if outcome_term_word_count == len(outcome_term_words):
                        outcome_term_FOUND = True

                elif len(outcome_term_words) == 2:

                    exposure_event_url_split_by_punc = re.findall(r"[\w']+|[.,!?;]", exposure_event_url)
                    for w, word in enumerate(exposure_event_url_split_by_punc):
                        for ow, outcome_term_word in enumerate(outcome_term_words):
                            if ow == 0 and outcome_term_word in word:
                                if w + 1 < len(exposure_event_url_split_by_punc) and outcome_term_words[ow + 1] in \
                                        exposure_event_url_split_by_punc[w + 1]:
                                    outcome_term_FOUND = True

                    # If term not found in URL, check for at least one word in the event code description
                    if not outcome_term_FOUND:
                        if any(term_word in exposure_event_code_desc for term_word in outcome_term_words):
                            outcome_term_FOUND = True

                    # # Generate regex to find multi-word term in article URL or event description
                    # # Doing this to avoid the following situation:
                    # # term = "share value", article queried has phrase "shared values"
                    # regex = r"(?i)^"
                    # for word in outcome_term_words:
                    #     regex += r"(?=.*\b{}\b)".format(word)
                    # regex += r".*$"
                    # if len(re.findall(regex, exposure_event_url)) > 1 or len(
                    #         re.findall(regex, exposure_event_code_desc)) > 1:
                    #     outcome_term_FOUND = True
                    #     print(f'REGEX CHECK: risk term = "{outcome_term}", regex = {regex}')
                    #     print(f'event URL: {exposure_event_url}')
                    #     print(f'event code description: {exposure_event_code_desc}')

                elif len(outcome_term_words) == 1:
                    if outcome_term in exposure_event_url or exposure_event_url in exposure_event_code_desc:
                        outcome_term_FOUND = True

                if outcome_term_FOUND:
                    exposure_term_category_ALL_outcome_occurrences[exposure_category] = \
                        exposure_term_category_ALL_outcome_occurrences[exposure_category].append(event_row,
                                                                                                 ignore_index=True)
                    exposure_term_ALL_outcome_occurrences[exposure_term] = exposure_term_ALL_outcome_occurrences[
                        exposure_term].append(event_row, ignore_index=True)
                    exposure_term_individual_outcome_occurrences[exposure_term][outcome_term] = \
                        exposure_term_individual_outcome_occurrences[exposure_term][outcome_term].append(event_row,
                                                                                                         ignore_index=True)

    # Drop duplicates
    for exposure_category, df in exposure_term_category_ALL_outcome_occurrences.items():
        df.drop_duplicates(subset='sourceurl', keep="last", inplace=True)
    for exposure_term, df in exposure_term_ALL_outcome_occurrences.items():
        df.drop_duplicates(subset='sourceurl', keep="last", inplace=True)
    return exposure_term_category_ALL_outcome_occurrences, exposure_term_ALL_outcome_occurrences, exposure_term_individual_outcome_occurrences


def get_exposure_category_events_summary_ANY_outcome(exposure_type, outcome_type,
                                                     exposure_terms_dfs, exposure_category_events_ANY_outcome_dfs,
                                                     exposure_term_to_category_mapping,
                                                     outcome_term_to_category_mapping):
    exposure_category_events_ANY_outcome_summary = pd.DataFrame(
        columns=[f'{exposure_type}',
                 f'Count of {exposure_type} category events',
                 f'Count of {exposure_type} category events with ANY {outcome_type} term',
                 f'Count of {exposure_type} category events with NO {outcome_type} term',
                 f'% {exposure_type} category events with ANY {outcome_type} term',
                 f'% {exposure_type} category events with NO {outcome_type} term'])

    for exposure_category, events_df in exposure_category_events_ANY_outcome_dfs.items():

        # Find all terms in that exposure category and take their df totals
        count_events_with_exposure = 0
        for exp_term, categ in exposure_term_to_category_mapping.items():
            if categ == exposure_category:
                count_events_with_exposure += exposure_terms_dfs[exp_term]['df'].shape[0]

        count_events_with_exposure_and_outcome = events_df.shape[0]
        count_events_with_exposure_and_NO_outcome = count_events_with_exposure - count_events_with_exposure_and_outcome
        summary_row = {
            f'{exposure_type}': exposure_category,
            f'Count of {exposure_type} category events': count_events_with_exposure,
            f'Count of {exposure_type} category events with ANY {outcome_type} term': count_events_with_exposure_and_outcome,
            f'Count of {exposure_type} category events with NO {outcome_type} term': count_events_with_exposure_and_NO_outcome,
            f'% {exposure_type} category events with ANY {outcome_type} term': count_events_with_exposure_and_outcome / count_events_with_exposure * 100 if count_events_with_exposure > 0 else 0,
            f'% {exposure_type} category events with NO {outcome_type} term': count_events_with_exposure_and_NO_outcome / count_events_with_exposure * 100 if count_events_with_exposure > 0 else 0
        }
        exposure_category_events_ANY_outcome_summary = exposure_category_events_ANY_outcome_summary.append(summary_row,
                                                                                                           ignore_index=True)

    return exposure_category_events_ANY_outcome_summary


def get_exposure_term_events_summary_ANY_outcome(exposure_type, outcome_type,
                                                 exposure_terms_dfs, exposure_term_events_ANY_outcome_dfs,
                                                 exposure_term_to_category_mapping, outcome_term_to_category_mapping):
    exposure_term_events_ANY_outcome_summary = pd.DataFrame(
        columns=[f'{exposure_type}', f'{exposure_type} term',
                 f'Count of {exposure_type} term events',
                 f'Count of {exposure_type} term events with ANY {outcome_type} term',
                 f'Count of {exposure_type} term events with NO {outcome_type} term',
                 f'% {exposure_type} term events with ANY {outcome_type} term',
                 f'% {exposure_type} term events with NO {outcome_type} term'])

    for exposure_term, events_df in exposure_term_events_ANY_outcome_dfs.items():
        count_events_with_exposure = exposure_terms_dfs[exposure_term]['df'].shape[0]
        count_events_with_exposure_and_outcome = events_df.shape[0]
        count_events_with_exposure_and_NO_outcome = count_events_with_exposure - count_events_with_exposure_and_outcome
        summary_row = {
            f'{exposure_type}': exposure_term_to_category_mapping[exposure_term],
            f'{exposure_type} term': exposure_term,
            f'Count of {exposure_type} term events': count_events_with_exposure,
            f'Count of {exposure_type} term events with ANY {outcome_type} term': count_events_with_exposure_and_outcome,
            f'Count of {exposure_type} term events with NO {outcome_type} term': count_events_with_exposure_and_NO_outcome,
            f'% {exposure_type} term events with ANY {outcome_type} term': count_events_with_exposure_and_outcome / count_events_with_exposure * 100 if count_events_with_exposure > 0 else 0,
            f'% {exposure_type} term events with NO {outcome_type} term': count_events_with_exposure_and_NO_outcome / count_events_with_exposure * 100 if count_events_with_exposure > 0 else 0
        }
        exposure_term_events_ANY_outcome_summary = exposure_term_events_ANY_outcome_summary.append(summary_row,
                                                                                                   ignore_index=True)

    return exposure_term_events_ANY_outcome_summary


def get_exposure_term_events_summary_BY_outcome_term(exposure_type, outcome_type,
                                                     exposure_terms_dfs, exposure_term_events_by_outcome_term_dfs,
                                                     exposure_term_events_ANY_outcome_summary,
                                                     exposure_term_to_category_mapping,
                                                     outcome_term_to_category_mapping):
    exposure_term_events_BY_outcome_term_summary = pd.DataFrame(
        columns=[f'{exposure_type}', f'{exposure_type} term', f'{outcome_type}',
                 f'Count of {exposure_type} term events with {outcome_type} term',
                 f'Count of {exposure_type} term events WITHOUT {outcome_type} term',
                 f'% {exposure_type} term events with {outcome_type} term',
                 f'% {exposure_type} term events WITHOUT {outcome_type} term',
                 f'Count of {exposure_type} term events'
                 ])

    for exposure_term, outcome_term_info in exposure_term_events_by_outcome_term_dfs.items():
        count_of_exposure_term_events = exposure_term_events_ANY_outcome_summary[
            exposure_term_events_ANY_outcome_summary[f'{exposure_type} term'] == exposure_term][
            [f'Count of {exposure_type} term events']].values[0][0]

        for outcome_term, outcome_term_events_df in outcome_term_info.items():
            count_of_outcome_term_events_for_exposure = outcome_term_events_df.shape[0]
            count_of_NO_outcome_term_events_for_exposure = count_of_exposure_term_events - count_of_outcome_term_events_for_exposure

            summary_row = {
                f'{exposure_type}': exposure_term_to_category_mapping[exposure_term],
                f'{exposure_type} term': exposure_term,
                f'{outcome_type}': outcome_term,
                f'Count of {exposure_type} term events with {outcome_type} term': count_of_outcome_term_events_for_exposure,
                f'Count of {exposure_type} term events WITHOUT {outcome_type} term': count_of_NO_outcome_term_events_for_exposure,
                f'% {exposure_type} term events with {outcome_type} term': count_of_outcome_term_events_for_exposure / count_of_exposure_term_events * 100 if count_of_exposure_term_events > 0 else 0,
                f'% {exposure_type} term events WITHOUT {outcome_type} term': count_of_NO_outcome_term_events_for_exposure / count_of_exposure_term_events * 100 if count_of_exposure_term_events > 0 else 0,
                f'Count of {exposure_type} term events': count_of_exposure_term_events
            }

            exposure_term_events_BY_outcome_term_summary = exposure_term_events_BY_outcome_term_summary.append(
                summary_row, ignore_index=True)

    return exposure_term_events_BY_outcome_term_summary


# TODO: Revise quantity calculations
# def get_odds_ratio_matrix_values(exposure_type, outcome_type, exposure_terms, outcome_terms,
#                                  exposure_term_events_by_outcome_term_summary,
#                                  outcome_term_events_by_exposure_term_summary,
#                                  total_db_event_count=103522599):
#     odds_ratio_values = pd.DataFrame(columns=[
#         'Exposure term', 'Outcome term', 'Odds ratio',
#         'Quantity 1', 'Quantity 2', 'Quantity 3', 'Quantity 4'
#     ])
#
#     for exposure_term in exposure_terms:
#         for outcome_term in outcome_terms:
#             # E.g. practice is exposure, risk is outcome: P(risk | practice) = "% Practice term events with Risk term"
#             exposure_filter_1 = (exposure_term_events_by_outcome_term_summary[f'{exposure_type} term'] == exposure_term)
#             outcome_filter_1 = (exposure_term_events_by_outcome_term_summary[f'{outcome_type}'] == outcome_term)
#             exposure_outcome_row = exposure_term_events_by_outcome_term_summary[exposure_filter_1 & outcome_filter_1]
#
#             ### QUANTITY 1
#             # Dividing by 100 at the end to get proper decimal, since it was multiplied by 100 in summary df
#             quantity_1 = exposure_outcome_row[[f'% {exposure_type} term events with {outcome_type} term']].values[0][
#                              0] / 100
#
#             ### QUANTITY 2
#             quantity_2 = \
#                 exposure_outcome_row[[f'% {exposure_type} term events WITHOUT {outcome_type} term']].values[0][
#                     0] / 100
#
#             outcome_filter_2 = (outcome_term_events_by_exposure_term_summary[f'{outcome_type} term'] == outcome_term)
#             exposure_filter_2 = (outcome_term_events_by_exposure_term_summary[f'{exposure_type}'] == exposure_term)
#             outcome_exposure_row = outcome_term_events_by_exposure_term_summary[outcome_filter_2 & exposure_filter_2]
#
#             ### QUANTITY 3
#             quantity_3 = \
#                 outcome_exposure_row[[f'% {outcome_type} term events WITHOUT {exposure_type} term']].values[0][
#                     0] / 100
#
#             ### QUANTITY 4
#             count_all_exposure_term_events = exposure_outcome_row[exposure_filter_1 & outcome_filter_1][
#                 [f'Count of {exposure_type} term events']].values[0][0]
#             count_of_outcome_term_events_WITHOUT_exposure_term = \
#                 outcome_exposure_row[[f'Count of {outcome_type} term events WITHOUT {exposure_type} term']].values[0][0]
#             quantity_4 = (
#                                  total_db_event_count - count_all_exposure_term_events) - count_of_outcome_term_events_WITHOUT_exposure_term
#
#             ### ODDS RATIO
#             odds_ratio = (quantity_1 / quantity_2) / (quantity_3 / quantity_4)
#
#             row = {
#                 'Exposure term': exposure_term,
#                 'Outcome term': outcome_term,
#                 'Odds ratio': odds_ratio,
#                 'Quantity 1': quantity_1,
#                 'Quantity 2': quantity_2,
#                 'Quantity 3': quantity_3,
#                 'Quantity 4': quantity_4,
#             }
#
#             odds_ratio_values = odds_ratio_values.append(row, ignore_index=True)
#
#     return odds_ratio_values

def get_total_events_for_exposure_terms_and_outcome_terms(
        exposure_terms_lst, outcome_terms_lst,
        exposure_term_dfs, outcome_term_dfs,
        exposure_term_events_by_outcome_term_events_df,
        outcome_term_events_by_exposure_term_events_df,
        id_col='globaleventid', date_col='monthyear', year_col='year', url_col='sourceurl'):
    """New totals need to be generated, now that event code descriptions have been factored into
    recognizing an article as belonging to a term, i.e. finding 'boycott' articles for
    'contingent worker' using event code descriptions of 'contingent worker' articles
    (events queried by the word 'boycott' will not be here, since 'boycott' articles were queried
    by URL and actor codes, not event code descriptions, since event code descriptions were joined
    AFTER querying events by terms.)) """

    total_events_exposure_terms = {exp_term: 0 for exp_term in exposure_terms_lst}

    for exposure_term in exposure_terms_lst:
        # Get the events for all of the outcome terms, where that exposure term exists
        exposure_events_found_in_outcome_term_events = pd.DataFrame(columns=[id_col, date_col, year_col, url_col])
        for outcome_term in outcome_terms_lst:
            tmp = outcome_term_events_by_exposure_term_events_df[outcome_term][exposure_term][
                [id_col, date_col, year_col, url_col]]
            exposure_events_found_in_outcome_term_events = exposure_events_found_in_outcome_term_events.append(tmp,
                                                                                                               ignore_index=True)

        # Add those to events queried by the exposure term
        all_exposure_events = exposure_events_found_in_outcome_term_events.append(
            exposure_term_dfs[exposure_term]['df'][[id_col, date_col, year_col, url_col]], ignore_index=True)
        all_exposure_events.drop_duplicates(url_col, keep='last', inplace=True)
        total_events_exposure_terms[exposure_term] = all_exposure_events.shape[0]

    total_events_outcome_terms = {out_term: 0 for out_term in outcome_terms_lst}

    for outcome_term in outcome_terms_lst:
        # Get the events for all of the exposure terms, where that outcome term exists
        outcome_events_found_in_exposure_term_events = pd.DataFrame(columns=[id_col, date_col, year_col, url_col])
        for exposure_term in exposure_terms_lst:
            tmp = exposure_term_events_by_outcome_term_events_df[exposure_term][outcome_term][
                [id_col, date_col, year_col, url_col]]
            outcome_events_found_in_exposure_term_events = outcome_events_found_in_exposure_term_events.append(tmp,
                                                                                                               ignore_index=True)

        # Add those to events queried by the outcome term
        all_outcome_events = outcome_events_found_in_exposure_term_events.append(
            outcome_term_dfs[outcome_term]['df'][[id_col, date_col, year_col, url_col]], ignore_index=True)
        all_outcome_events.drop_duplicates(url_col, keep='last', inplace=True)
        total_events_outcome_terms[outcome_term] = all_outcome_events.shape[0]

    return total_events_exposure_terms, total_events_outcome_terms


def get_odds_ratio_quantities(
        exposure_type, outcome_type,
        exposure_terms_lst, outcome_terms_lst,
        exposure_term_dfs, outcome_term_dfs,
        exposure_term_events_by_outcome_term_events_df,
        outcome_term_events_by_exposure_term_events_df,
        id_col='globaleventid', date_col='monthyear', year_col='year', url_col='sourceurl',
        total_db_event_count=103522599):
    # Comments below are given that: exposure = practice, outcome = risk
    odds_ratio_values_df = pd.DataFrame(columns=
                                        ['Exposure type', 'Outcome type',
                                         'Exposure term', 'Outcome term',
                                         'ODDS RATIO',
                                         'Total exposure events', 'Total outcome events',
                                         'Total events with exposure AND outcome (A)',  # a)
                                         'Total events with outcome AND NO exposure (B)',  # b)
                                         'Total events with exposure AND NO outcome (C)',  # c)
                                         'Total events with NO exposure AND NO outcome (D)'  # d)
                                         ])

    # Get total events for each term (Not the same as totals based on events queried by that term)
    total_events_exposure_terms, total_events_outcome_terms = get_total_events_for_exposure_terms_and_outcome_terms(
        exposure_terms_lst, outcome_terms_lst,
        exposure_term_dfs, outcome_term_dfs,
        exposure_term_events_by_outcome_term_events_df,
        outcome_term_events_by_exposure_term_events_df)

    for exp_term in exposure_terms_lst:
        for out_term in outcome_terms_lst:

            odds_ratio_df_row = {
                'Exposure type': exposure_type,
                'Outcome type': outcome_type,
                'Exposure term': exp_term,
                'Outcome term': out_term,
                'Total exposure events': total_events_exposure_terms[exp_term],
                'Total outcome events': total_events_outcome_terms[out_term]}

            exp_and_out_1 = exposure_term_events_by_outcome_term_events_df[exp_term][out_term]
            exp_and_out_2 = outcome_term_events_by_exposure_term_events_df[out_term][exp_term]
            exp_and_out_events = exp_and_out_1.append(exp_and_out_2, ignore_index=False)
            exp_and_out_events.drop_duplicates(url_col, keep='last', inplace=True)

            odds_ratio_df_row['Total events with exposure AND outcome (A)'] = exp_and_out_events.shape[0]
            odds_ratio_df_row['Total events with outcome AND NO exposure (B)'] = odds_ratio_df_row[
                                                                                     'Total outcome events'] - \
                                                                                 odds_ratio_df_row[
                                                                                     'Total events with exposure AND outcome (A)']
            odds_ratio_df_row['Total events with exposure AND NO outcome (C)'] = odds_ratio_df_row[
                                                                                     'Total exposure events'] - \
                                                                                 odds_ratio_df_row[
                                                                                     'Total events with exposure AND outcome (A)']
            odds_ratio_df_row['Total events with NO exposure AND NO outcome (D)'] = total_db_event_count - \
                                                                                    odds_ratio_df_row[
                                                                                        'Total exposure events'] - \
                                                                                    odds_ratio_df_row[
                                                                                        'Total outcome events']

            try:
                odds_ratio_df_row['ODDS RATIO'] = (odds_ratio_df_row['Total events with exposure AND outcome (A)'] /
                                                   odds_ratio_df_row[
                                                       'Total events with exposure AND NO outcome (C)']) / (
                                                          odds_ratio_df_row[
                                                              'Total events with outcome AND NO exposure (B)'] /
                                                          odds_ratio_df_row[
                                                              'Total events with NO exposure AND NO outcome (D)'])
            except ZeroDivisionError:
                odds_ratio_df_row['ODDS RATIO'] = -999  # no articles with outcome term...
            odds_ratio_values_df = odds_ratio_values_df.append(odds_ratio_df_row, ignore_index=True)

    return odds_ratio_values_df


def get_heatmap_data(exposure_terms, outcome_terms, odds_ratio_values_df):
    heatmap_data = []
    odds_ratio_values = odds_ratio_values_df[odds_ratio_values_df['ODDS RATIO'] != -999]
    for exposure_term in exposure_terms:
        exposure_term_odds_ratios = odds_ratio_values[odds_ratio_values['Exposure term'] == exposure_term]
        outcomes_term_odds_ratios_for_exposure_term = []

        for outcome_term in outcome_terms:
            outcome_term_row = exposure_term_odds_ratios[exposure_term_odds_ratios['Outcome term'] == outcome_term]
            outcomes_term_odds_ratio_for_exposure_term = outcome_term_row['ODDS RATIO'].values[0]
            outcomes_term_odds_ratios_for_exposure_term.append(outcomes_term_odds_ratio_for_exposure_term)

        heatmap_data.append(outcomes_term_odds_ratios_for_exposure_term)

    return heatmap_data


def generate_heatmap(exposure_type, outcome_type, exposure_terms, outcome_terms, heatmap_values):
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_values,
        x=outcome_terms,
        y=exposure_terms,
        hoverongaps=False,
        colorscale='viridis',
        # reversescale=True
    ))
    fig.update_layout(
        xaxis_title=f"{outcome_type}",
        yaxis_title=f"{exposure_type}",
        autosize=False,
        width=1000,
        height=1000,
        title_text=f"Odds Ratio Test for GDELT Events. {exposure_type.title()} (Exposure), {outcome_type.title()} (Outcome)",
        title_x=0.5)
    return fig
