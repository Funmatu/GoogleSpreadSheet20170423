# coding: utf-8
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def read(String=None):
    scope = ['https://spreadsheets.google.com/feeds']
    doc_id = ''
    path = os.path.abspath(r".json")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path, scope)
    client = gspread.authorize(credentials)
    gfile   = client.open_by_key(doc_id)
    if String is None:
        worksheet = gfile.sheet1
    else:
        worksheet = gfile.worksheet(String)

    records = worksheet.get_all_values()

    return records

#多分5万セルを超えるとネットワークエラー（413 Request Entity Too Large）
def write(DATA, String=None, index=None):
    scope = ['https://spreadsheets.google.com/feeds']
    doc_id = ''
    path = os.path.abspath(r".json")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path, scope)
    client = gspread.authorize(credentials)
    gfile   = client.open_by_key(doc_id)
    
    Index     = DATA.index
    Columns = DATA.columns
    Nind = len(Index)
    Ncol = len(Columns)
    DATA.index    = [i for i in range(Nind)]
    DATA.columns= [i for i in range(Ncol)]
 
    if index is None:
        if String is None:
            worksheet = gfile.sheet1
        else:
            worksheet = gfile.add_worksheet(String, Nind, Ncol)
        # Select a range
        cell_list = worksheet.range(1, 1, Nind, Ncol)#ex. 'A1:C7'
        col_list = [i for i in range(Ncol)]*Nind
        ind_list = [i for i in range(Nind) for j in range(Ncol)]        
        for (cell, col, ind) in zip(cell_list, col_list, ind_list):
            cell.value = DATA[col][ind]
        # Update in batch
        worksheet.update_cells(cell_list)
    else: 
        if String is None:
            worksheet = gfile.sheet1
        else:
            worksheet = gfile.add_worksheet(String, Nind+1, Ncol+1)     
        #set the index name
        [worksheet.update_cell(1, 1, Index.name)]       
        # Select a range
        cell_list = worksheet.range(1, 2, 1, Ncol+1)
        col_list = [i for i in range(Ncol)]*Nind
        for (cell, col) in zip(cell_list, col_list):
            cell.value = Columns[col]
        # Update in batch
        worksheet.update_cells(cell_list)
        # Select a range
        cell_list = worksheet.range(2, 1, Nind+1, 1)
        ind_list = [i for i in range(Nind)]
        for (cell, ind) in zip(cell_list, ind_list):
            cell.value = Index[ind]
        # Update in batch
        worksheet.update_cells(cell_list)

        # Select a range
        cell_list = worksheet.range(2, 2, Nind+1, Ncol+1)
        col_list = [i for i in range(Ncol)]*Nind
        ind_list = [i for i in range(Nind) for j in range(Ncol)]
        for (cell, col, ind) in zip(cell_list, col_list, ind_list):
            cell.value = DATA[col][ind]
        # Update in batch
        if (Nind * Ncol) < 50000:
            worksheet.update_cells(cell_list)
        else: #50,000セル以上の場合は48,000ごとにバッチ処理
            loopcnt = len(cell_list) // 48000 + 1
            for i in range(loopcnt):
                worksheet.update_cells(cell_list[i*48000:(i+1)*48000])
        
    
if __name__ == '__main__':
    #df = read()
    write(dfdf, 'sheet3', 1)