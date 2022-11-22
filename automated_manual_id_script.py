import pandas as pd
import re
import ast
from os import listdir
from ftfy import fix_text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
import pandas.io.formats.excel
from configparser import ConfigParser
from datetime import datetime
from termcolor import colored, cprint

class automated_manual_id():

    def write_excel_reformat(self, df, filename, sheetname):

        pandas.io.formats.excel.header_style = None
        row_count = df['article_title'].count()
        writer_object = pd.ExcelWriter(filename, engine='xlsxwriter')
        writer_object.book.strings_to_urls = False
        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer_object, sheet_name=sheetname, index=False)
        # Close the Pandas Excel writer and output the Excel file.
        workbook_object = writer_object.book
        worksheet_object = writer_object.sheets[sheet_name]
        worksheet_object.set_tab_color('#538ED5')
        worksheet_object.set_column('A:A', 10)  # Abstract ID
        worksheet_object.set_column('B:B', 10)  # Manual ID
        worksheet_object.set_column('C:C', 40)  # Title
        worksheet_object.set_column('D:D', 30)  # Link
        worksheet_object.set_column('E:E', 30)  # Authors
        worksheet_object.set_column('F:F', 30)  # Author Affiliation
        worksheet_object.set_column('G:G', 60)  # Abstract Text
        worksheet_object.set_column('H:H', 10)  # Date
        worksheet_object.set_column('I:I', 10)  # Start Time
        worksheet_object.set_column('J:J', 10)  # End Time
        worksheet_object.set_column('K:K', 30)  # Location
        worksheet_object.set_column('L:L', 30)  # Session Title
        worksheet_object.set_column('M:M', 30)  # Session Type
        worksheet_object.set_column('N:N', 60)  # Clinical Trial Identification
        worksheet_object.set_column('O:O', 60)  # Funding
        worksheet_object.set_column('P:P', 60)  # Disclosure

        worksheet_object.freeze_panes(1, 0)
        header_format = workbook_object.add_format(
            {'bold': True, 'bg_color': '#4F81BD', 'font_name': 'Arial', 'font_size': 9, 'align': 'center',
             'valign': 'center', 'border': 1})
        data_format1 = workbook_object.add_format(
            {'border': 1, 'bg_color': '#B8CCE4', 'font_name': 'Arial', 'font_size': 8,
             'align': 'left', 'valign': 'top'})
        data_format2 = workbook_object.add_format(
            {'border': 1, 'bg_color': '#DBE5F1', 'font_name': 'Arial', 'font_size': 8,
             'align': 'left', 'valign': 'top'})

        worksheet_object.set_row(0, cell_format=header_format)

        for row in range(1, row_count+1):
            if row%2 == 0:
                worksheet_object.set_row(row, cell_format=data_format1)
            else:
                worksheet_object.set_row(row, cell_format=data_format2)

        writer_object.save()

    def zeta0_creation(self, indexed_files_dir):
        """ Returns pandas dataframe which has latest record for each manual id after merging all "sheet_name"
        in the previously indexed_files which are present in "indexed_files_dir"
        """
        indexed_files = [file for file in listdir(indexed_files_dir) if not file.startswith("~")]

        indexed_files_dict = {}
        indexed_files_dict.clear()

        dateList = []
        del dateList[:]
        for file in indexed_files:
            dated = file.split('_')[-1].split('.')[0]
            dated = dated[4:] + dated[:4]
            dateList.append(dated)
            indexed_files_dict[dated] = file

        dataframes = {}

        for dated, file in indexed_files_dict.items():
            file_name = indexed_files_dir + '\\' + file
            dataframes[dated] = pd.read_excel(file_name, sheet_name=0,dtype=str)
            dataframes[dated]['file_date'] = dated
            dataframes[dated]['mid'] = [int(elem.split('_')[-1]) for elem in dataframes[dated]['manual_id']]

        merged_df = pd.concat([dataframes[dated] for dated in dateList], ignore_index=True)
        merged_df = merged_df.sort_values('file_date', ascending=False)
        zeta0 = merged_df.drop_duplicates(subset='manual_id', keep='first')
        pd.set_option('mode.chained_assignment', None)
        zeta0 = zeta0.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        zeta0['article_title'] = zeta0['article_title'].str.lower()
        zeta0 = zeta0.sort_values('mid', ascending=True)
        return zeta0

    """     BELOW FUNCTION IS NOT IN USE NOW, removing duplicate authors+chairs is part of PAIRTRON Script now.
    def unique_in_lists(self, list1, list2):
        list1_unique = [elem for elem in list1 if elem not in ';'.join(list2)]
        list2_unique = [elem for elem in list2 if elem not in ';'.join(list1)]
        common = [elem for elem in list1 if elem in list2]
        union_list = list1_unique + common + list2_unique
        return ';'.join(union_list)"""

    def concat_columns(self, col1, col2):
        if col1 != "" and col1.lower() != "nan" and col2 != "" and col2.lower() != "nan":
            output = ';'.join(col1.split(';') + col2.split(';'))
        elif col1 != "" and col1.lower() != "nan":
            output = col1
        elif col2 != "" and col2.lower() != "nan":
            output = col2
        else:
            output = ""
        return output

    def remove_extra_spaces(self, string_val):
        if type(string_val) == str:
            string_val = ' '.join([elem.strip() for elem in string_val.split(' ') if elem.strip() != ''])
        return string_val

    def remove_extra_spaces_from_col(self, col):
        col = [self.remove_extra_spaces(elem) for elem in col]
        return col

    def reformat_recently_indexed(self, recently_indexed, columns):
        indexed_df = pd.read_excel(recently_indexed, sheet_name=0, dtype = str)
        indexed_df = indexed_df.replace('nan', '')
        indexed_df_columns = indexed_df.columns.tolist()
        df_columns = {}
        for elem in indexed_df_columns:
            df_columns[elem] = elem.lower()
        indexed_df = indexed_df.rename(columns=df_columns)
        indexed_df_columns = [elem.lower() for elem in indexed_df_columns]
        indexed_df_columns.remove('source_id')
        indexed_df[indexed_df_columns] = indexed_df[indexed_df_columns].fillna('').astype(str)
        indexed_df = indexed_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        indexed_df = indexed_df.apply(lambda x: self.remove_extra_spaces_from_col(x) if x.dtype == "object" else x)
        if 'chairs' in indexed_df.columns:
            indexed_df['authors'] = indexed_df[['chairs', 'authors']].apply(lambda x: self.concat_columns(str(x[0]),str(x[1])),axis=1)
        if 'chair_affiliation' in indexed_df.columns:
            indexed_df['author_affiliation'] = indexed_df[['chair_affiliation', 'author_affiliation']].apply(lambda x: self.concat_columns(str(x[0]),str(x[1])),axis=1)
        indexed_df['mid_level'] = ''
        columns_added = columns + ['mid_level']
        return pd.DataFrame(indexed_df, columns=columns_added)

    def alert_for_probable_duplicates(self, recently_indexed_df):
        recently_indexed_df = recently_indexed_df[recently_indexed_df.duplicated(['zeta0_article_title', 'url', 'authors', 'date', 'start_time', 'end_time', 'session_title'], keep=False)]
        recently_indexed_df = recently_indexed_df[recently_indexed_df['zeta0_article_title'] != '']
        if len(recently_indexed_df['article_title']) == 0:
            cprint("All good, no duplicates found in data.\n", 'green', attrs=['bold'])
        else:
            cprint("ALERT: {} probable duplicates found in newly indexed file and are written to duplicates.csv\n".format(recently_indexed_df['article_title'].count()), 'red', attrs=['bold'])
            recently_indexed_df.to_csv('duplicates.csv', index=False)

    def ngrams(self, string_var, n=3):
        string_var = fix_text(string_var)  # fix text encoding issues
        string_var = string_var.encode("ascii", errors="ignore").decode()  # remove non ascii chars
        string_var = string_var.lower()  # make lower case
        chars_to_remove = [")", "(", ".", "|", "[", "]", "{", "}", "'"]
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        string_var = re.sub(rx, '', string_var)  # remove the list of chars defined above
        string_var = string_var.replace('&', 'and')
        string_var = string_var.replace(',', ' ')
        string_var = string_var.replace('-', ' ')
        string_var = string_var.title()  # normalise case - capital at start of each word
        string_var = re.sub(' +', ' ', string_var).strip()  # get rid of multiple spaces and replace with a single space
        string_var = ' ' + string_var + ' '  # pad names for ngrams...
        string_var = re.sub(r'[,-./]|\sBD', r'', string_var)
        ngrams = zip(*[string_var[i:] for i in range(n)])
        return [''.join(ngram) for ngram in ngrams]

    def getNearestN(self, query, vectorizer, nbrs):
        queryTFIDF_ = vectorizer.transform(query)
        distances, indices = nbrs.kneighbors(queryTFIDF_)
        return distances, indices

    def threshold_check(self, row, column_name, threshold):
        return row['zeta0_' + column_name] if float(row['match_confidence_' + column_name]) < threshold else ''

    def nearestNeighbor_string_match(self, current_df , zeta0_df, column_name, threshold=0.2):
        zeta0_name = zeta0_df[column_name].astype(str)
        current_name = current_df[column_name].astype(str)
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tfidf = vectorizer.fit_transform(zeta0_name)

        nbrs = NearestNeighbors(n_neighbors=1, n_jobs=-1).fit(tfidf)
        unique_org = set(current_name.values)  # set used for increased performance
        distances, indices = self.getNearestN(unique_org, vectorizer, nbrs)
        unique_org = list(unique_org)  # need to convert back to a list
        matches = []
        for i, j in enumerate(indices):
            temp = [round(distances[i][0], 2), zeta0_name.values[j][0], unique_org[i]]
            matches.append(temp)
        #print("Note: match_confidence (lower is better)")
        matched_df = pd.DataFrame(matches,
                               columns=['match_confidence_' + column_name, 'zeta0_' + column_name , column_name])
        matched_df['zeta0_' + column_name] = matched_df.apply(lambda row: self.threshold_check(row, column_name, threshold), axis=1)
        if 'session_title' == column_name:
            matched_df.drop(['match_confidence_'+column_name], axis = 1, inplace = True)
        return pd.merge(current_df, matched_df, left_on=column_name, right_on=column_name, how='inner')

    def nearestNeighbor_match_add_columns(self, df1, df2, threshold=0.8):
        """ df1- newly indexed
            df2- Zeta0
        """
        df1 = self.nearestNeighbor_string_match(df1, df2, 'article_title', threshold)
        df1 = self.nearestNeighbor_string_match(df1, df2, 'session_title', threshold)
        return df1

    def merge_manual_ids(self, row):
        row['manual_id'] = row['manual_id_y'] if ((row['manual_id'] == "") and
                               (row['manual_id_y'] != "")) else row['manual_id']
        return row

    def analyse_unassigned_articles(self, joined_df_level, zeta0_df, unassigned_manual_ids):
        zeta0_df = zeta0_df[zeta0_df['manual_id'].isin(unassigned_manual_ids)]
        filtered_df = joined_df_level[joined_df_level['manual_id'] == '']
        filtered_df_join_zeta0 = pd.merge(filtered_df, zeta0_df, left_on=['zeta0_article_title'],
                                    right_on=['article_title'], how='left')
        filtered_df_join_zeta0.columns = filtered_df_join_zeta0.columns.str.replace('_x', '')
        filtered_df_join_zeta0.columns = filtered_df_join_zeta0.columns.str.replace('_y','_zeta0')
        filtered_df_join_zeta0 = pd.DataFrame(filtered_df_join_zeta0,
                            columns = ['source_id', 'source_id_zeta0', 'article_title', 'zeta0_article_title', 'url', 'date', 'date_zeta0', 'start_time', 'start_time_zeta0', 'end_time', 'end_time_zeta0', 'manual_id_zeta0'])
        filtered_df_join_zeta0.columns = filtered_df_join_zeta0.columns.str.replace('zeta0_article_title',
                                                                                    'article_title_zeta0')
        match_found = filtered_df_join_zeta0[filtered_df_join_zeta0['manual_id_zeta0'].notnull()]
        cprint("\nAnalysis completed:", 'blue', attrs=['bold'])
        cprint("Compared {} article_title and written to analysed_records_match_found.csv".format(match_found['article_title'].count()), 'blue')
        match_found.to_csv("analysed_records_match_found.csv", index=False)
        no_match_found = filtered_df_join_zeta0[filtered_df_join_zeta0['manual_id_zeta0'].isnull()]
        cprint("Unable to compare {} article_title and written to analysed_records_match_not_found.csv".format(no_match_found['article_title'].count()), 'blue')
        no_match_found.to_csv("analysed_records_match_not_found.csv", index=False)

    def get_manual_id_on_column_set(self, level, joined_df_level_x, zeta0_df, assigned_manual_id_level_x,
                                    unassigned_manual_id_level_x, columns_list, columns_zeta_added, source_id_df):

        columns_list_x = []
        joined_df_level_x = joined_df_level_x.fillna('')
        zeta0_df = zeta0_df.fillna('')
        joined_df_level_x_unassigned = joined_df_level_x[joined_df_level_x['manual_id'] == '']
        joined_df_level_x_assigned = joined_df_level_x[joined_df_level_x['manual_id'] != '']

        for elem in columns_list:
            if elem in ['article_title', 'session_title']:
                columns_list_x.append('zeta0_' + elem)
            else:
                columns_list_x.append(elem)

        if 'source_id' in columns_list:
            zeta0_df = source_id_df
        # print(columns_list)
        joined_df_level_y = pd.merge(joined_df_level_x_unassigned,
                                     zeta0_df[zeta0_df['manual_id'].isin(list(unassigned_manual_id_level_x))],
                                     left_on=columns_list_x,
                                     right_on=columns_list, how='left')
        joined_df_level_y.columns = joined_df_level_y.columns.str.replace('_x', '')
        joined_df_level_y = joined_df_level_y.fillna('')
        joined_df_level_y = joined_df_level_y.apply(lambda row: self.merge_manual_ids(row), axis=1)
        joined_df_level_y = joined_df_level_y.drop_duplicates(subset=columns_zeta_added, keep='first')
        assigned_manual_id_level_y = set(joined_df_level_y['manual_id'])
        if '' in assigned_manual_id_level_y:
            assigned_manual_id_level_y.remove('')
        print("assigned manual_id's level {}:".format(level+1), len(assigned_manual_id_level_y - assigned_manual_id_level_x))
        joined_df_level_y['mid_level'] = str(level+1)
        unassigned_manual_id_level_y = unassigned_manual_id_level_x - assigned_manual_id_level_y
        joined_df_level_y['mid_level'].mask(joined_df_level_y['manual_id'] == '', '', inplace=True)
        joined_df_level_y.drop([col for col in joined_df_level_y.columns if col.endswith("_y")], axis=1, inplace=True)
        joined_df_level_y = pd.concat([joined_df_level_x_assigned, joined_df_level_y])
        return (joined_df_level_y, assigned_manual_id_level_y, unassigned_manual_id_level_y)

    def extract_manual_id(self, indexed_df, zeta0_df, columns_zeta_added, fetch_mid_by_levels):

        print("Total articles in indexed file: ", indexed_df['article_title'].count())
        source_id_df = zeta0_df[zeta0_df['source_id'].notnull()]
        source_id_df = source_id_df.fillna('')
        indexed_df = indexed_df.fillna('')
        source_id_df_columns = source_id_df.columns.tolist()
        source_id_df_columns.remove('source_id')
        source_id_df[source_id_df_columns] = source_id_df[source_id_df_columns].astype(str)
        indexed_df_columns = indexed_df.columns.tolist()
        indexed_df_columns.remove('source_id')
        indexed_df[indexed_df_columns] = indexed_df[indexed_df_columns].astype(str)

        columns_list = fetch_mid_by_levels[0]
        columns_list_x = []
        for elem in columns_list:
            if elem in ['article_title', 'session_title']:
                columns_list_x.append('zeta0_' + elem)
            else:
                columns_list_x.append(elem)

        joined_df_level = pd.merge(indexed_df, source_id_df, left_on=columns_list_x,
                                    right_on=columns_list, how='left')
        joined_df_level.columns = joined_df_level.columns.str.replace('_x', '')
        zeta0_manual_ids = set(zeta0_df['manual_id'])
        print("zeta0 manual IDs:",len(zeta0_manual_ids))
        assigned_manual_id_level = set(joined_df_level['manual_id'])
        assigned_manual_id_level = {x for x in assigned_manual_id_level if x == x}
        print("\nassigned manual_id's level 1:",len(assigned_manual_id_level))
        joined_df_level[joined_df_level['manual_id'] != '']['mid_level'] = 1
        unassigned_manual_id_level = set(zeta0_df['manual_id']) - assigned_manual_id_level
        joined_df_level.drop([col for col in joined_df_level.columns if col.endswith("_y")], axis=1, inplace=True)
        if len(unassigned_manual_id_level) == 0:
            print("Wow! all articles are tagged to manual_id's")
            return joined_df_level

        compare_by_columns_level = fetch_mid_by_levels[1:]

        for i in range(len(compare_by_columns_level)):
            joined_df_level, assigned_manual_id_level, unassigned_manual_id_level = self.get_manual_id_on_column_set(i+1,
            joined_df_level, zeta0_df, assigned_manual_id_level,
            unassigned_manual_id_level, compare_by_columns_level[i], columns_zeta_added, source_id_df)

            if len(unassigned_manual_id_level) == 0:
                print("Wow! all articles are tagged to manual_id's")
                return (joined_df_level,unassigned_manual_id_level)

        count_tagged = joined_df_level[joined_df_level['manual_id'] != '']['article_title'].count()
        count_untagged = joined_df_level[joined_df_level['manual_id'] == '']['article_title'].count()
        print("Note: {} articles tagged to manual_id and {} are still untagged. Sum: {}\n".format(count_tagged,count_untagged,count_tagged+count_untagged))
        return (joined_df_level, unassigned_manual_id_level)

    def insert_incremental_manual_id(self, sorted_df, conference_name, start_value):
        pd.set_option('mode.chained_assignment', None)
        mid_exists = sorted_df.loc[sorted_df['mid'].notnull()]
        mid_not_exists = sorted_df.loc[sorted_df['mid'].isnull()]
        mid_not_exists.index = range(len(mid_not_exists.index))
        mid_not_exists.loc[:,'mid'] = start_value + mid_not_exists.loc[:,'mid'].index
        mid_not_exists['manual_id'] = mid_not_exists.apply(lambda row: conference_name + "_" + str(row['mid']), axis=1)
        return pd.concat([mid_exists, mid_not_exists], ignore_index=True)

    def duplicate_mids(self, dfObj):
        duplicate_mids_set = set(dfObj[dfObj.duplicated(['manual_id'])]['manual_id'])
        if '' in duplicate_mids_set:
            duplicate_mids_set.remove('')
        if len(duplicate_mids_set)>0:
            cprint("\nALERT: {} Duplicate MIDs found: {}".format(len(duplicate_mids_set),duplicate_mids_set), 'red', attrs=['bold'])
        else:
            cprint("\nNo Duplicate MIDs. Good to go ahead", 'green', attrs=['bold'])

    def remove_dup_mid_exact_match(self, df):
        duplicate_mids_set = set(df[df.duplicated(['manual_id'])]['manual_id'])
        if '' in duplicate_mids_set:
            duplicate_mids_set.remove('')
        exact_match_set = set(df[df['match_confidence_article_title'] == '0.0']['manual_id'])
        exact_match_and_duplicate = duplicate_mids_set.intersection(exact_match_set)
        #print(len(exact_match_and_duplicate), exact_match_and_duplicate)
        for elem in exact_match_and_duplicate:
            df.loc[(df['manual_id'] == elem) & (df['match_confidence_article_title'] != '0.0'), 'manual_id'] = ''
        return df

    def process_manual_id(self, indexed_files_dir, sheet_name, recently_indexed, columns, columns_zeta_added, output_file_name, conference_name, fetch_mid_by_levels):
        """ Step 1: Zeta0 file creation from all previously indexed files if exist
            Step 2: reformat recently indexed file in ready2upload channel to get chairs and chair_affiliation
            details in authors and author_affiliation column resp.
            Step 3: Add extra columns by fuzzy string match into recently indexed file in step 2
            Step 4: Join files in Step 1 and Step 3 to assign manual_id to recently_indexed_file
            Step 5: Add incremental manual id for records in recently indexed file for which manual_id is still blank
        """
        indexed_files = [file for file in listdir(indexed_files_dir) if not file.startswith("~")]
        if len(indexed_files) != 0:
            # Step 1: Zeta0 creation
            zeta0 = self.zeta0_creation(indexed_files_dir)
            zeta0_df = pd.DataFrame(zeta0, columns=columns)

            #Step 2: reformat recently indexed file
            recently_indexed_df = self.reformat_recently_indexed(recently_indexed, columns)

            #Step 3: fuzzy string matching to create new columns like zeta0_article_title, zeta0_session_title in recently_indexed_file
            indexed_df_added_cols = self.nearestNeighbor_match_add_columns(recently_indexed_df, zeta0_df)
            indexed_df_added_cols = pd.DataFrame(indexed_df_added_cols, columns=columns_zeta_added)
            #indexed_df_added_cols.to_excel("indexed_df_added_cols.xlsx",index=False)
            self.alert_for_probable_duplicates(indexed_df_added_cols)

            #Step 4: Join indexed_df_added_cols with zeta0_df to get manual_id
            indexed_df_join_zeta0, unassigned_manual_ids = self.extract_manual_id(indexed_df_added_cols, zeta0_df, columns_zeta_added, fetch_mid_by_levels)
            indexed_df_join_zeta0.to_excel("indexed_df_join_zeta0.xlsx",index=False)
            #indexed_df_join_zeta0.drop(['mid_level'], axis=1, inplace=True)
            dedup_mid_exact_match = self.remove_dup_mid_exact_match(indexed_df_join_zeta0)
            ready_for_incremental_id = pd.DataFrame(dedup_mid_exact_match, columns=columns)

            # Step 5: Add incremental manual id
            start_value = max(zeta0_df['manual_id'].str.split('_').str[-1].astype(int)) + 1
            print("Initial value for manual_id will be: ", start_value)
            all_assigned = ready_for_incremental_id
            all_assigned['mid'] = all_assigned.apply(
                lambda row: int(row['manual_id'].split('_')[-1]) if row['manual_id'] != "" else None,
                axis=1)
            sorted_df = all_assigned.sort_values(by=['mid'])
            # UNCOMMENT below line if you want the code to assign the manual ID to the left out rows
            # sorted_df = self.insert_incremental_manual_id(sorted_df, conference_name, start_value)
            sorted_df.drop(['mid'], axis=1, inplace=True)
            self.write_excel_reformat(sorted_df, output_file_name, sheet_name)
            self.analyse_unassigned_articles(indexed_df_join_zeta0, zeta0_df, unassigned_manual_ids)
            self.duplicate_mids(sorted_df)
        else:
            print("This seems first index, no previously indexed_files provided")
            # Step 2: reformat recently indexed file
            ready_for_incremental_id = self.reformat_recently_indexed(recently_indexed, columns)
            ready_for_incremental_id.drop(['mid_level'], axis=1, inplace=True)
            ready_for_incremental_id = ready_for_incremental_id.fillna('')
            # Step 5: Add incremental manual id
            start_value = 1
            print("Initial value for manual_id will be: ", start_value)
            all_assigned = ready_for_incremental_id
            all_assigned['mid'] = all_assigned.apply(
                lambda row: int(row['manual_id'].split('_')[-1]) if row['manual_id'] != "" else None,
                axis=1)
            all_assigned = self.insert_incremental_manual_id(all_assigned, conference_name, start_value)
            all_assigned.drop(['mid'], axis=1, inplace=True)
            self.write_excel_reformat(all_assigned, output_file_name, sheet_name)

if __name__ == "__main__":

    start = datetime.now()
    print ("Script Start Time ",start)
    print ("Script Running.....\n")

    parser = ConfigParser()
    parser.read('automated_manual_id_config.ini')

    indexed_files_dir = parser.get('dynamic_fields', 'indexed_files_dir')
    sheet_name = parser.get('dynamic_fields', 'sheet_name')
    recently_indexed = parser.get('dynamic_fields', 'recently_indexed')

    output_file_name = recently_indexed.split('\\')[-1].split('.')[0] + '_MID.' + recently_indexed.split('\\')[-1].split('.')[1]
    conference_name = parser.get('dynamic_fields', 'manual_id_prefix')

    columns = parser.get('static_fields', 'columns').split(',')
    columns_zeta_added = parser.get('static_fields', 'columns_zeta_added').split(',')
    fetch_mid_by_levels = ast.literal_eval(parser.get('static_fields', 'fetch_mid_by_levels'))

    obj = automated_manual_id()
    obj.process_manual_id(indexed_files_dir, sheet_name, recently_indexed, columns, columns_zeta_added, output_file_name, conference_name, fetch_mid_by_levels)

    total_time = datetime.now() - start
    print ("\nScript End Time ",datetime.now())
    print ("Execution Time", total_time)