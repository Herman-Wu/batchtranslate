import os, requests, uuid, json
import pandas as pd 
import matplotlib as plt
import sklearn 
import seaborn as sns 
import argparse
def batch_translate(df, start_index, end_index, source_column_name, translated_column_name,subscriptionKey):

    if not df.columns.contains(translated_column_name):
        df[translated_column_name]=''
    
    ts_index=df.columns.get_loc(translated_column_name)
    column_index=df.columns.get_loc(source_column_name)
    #print (f"Translated Description index is {ts_index}")

    text_to_translates=df.iloc[start_index:end_index,column_index].values

    #print (f"Prepare to translate {len(text_to_translates)} sentenses")

    b_content=""
    bodys=[]

    for  txt in text_to_translates:

        # remove special charachters that broken JSON 
        # TODO: add more special charachters , and move this into a function 
        txt=str(txt).replace("\"",'').replace("\'",'').replace("\/",'').replace("\\",'').replace('\r', '').replace('\n', '')
        
        if (len(b_content)+len(txt))>5000:  # Based on translation API,  Max batch_size is 100, and the total characters are 5000
            bodys.append(b_content)
            b_content=""

        if len(b_content)>0:
            b_content+=" ,{ \"text\": \"%s\" }" %(txt) 
        else: 
            b_content+=" { \"text\": \"%s\" }" %(txt) 

    bodys.append(b_content)

    # WW translation API          
    #base_url = 'https://api.cognitive.microsofttranslator.com'

    # South-East Asia translate API
    base_url = 'https://api-apc.cognitive.microsofttranslator.com'

    #subscriptionKey='0ad3efa3c6d84d8eaa64c48394c23f56'
  
    headers = {
        'Ocp-Apim-Subscription-Key': subscriptionKey,
        'Content-type': 'application/json',
        'X-ClientTraceId': str(uuid.uuid4())
    }

    trans_content=[]

    for body_content in bodys:
            
        #print(f"[{body_content} ]")

        body2=json.loads(f"[{body_content} ]")
        #print (json.dumps(body2, sort_keys=True, indent=4,
        #                ensure_ascii=False, separators=(',', ': ')))

         # Detecting Language ....
        path = '/detect?api-version=3.0'
        constructed_url = base_url + path

        # You can pass more than one object in body.
        request = requests.post(constructed_url, headers=headers, json=body2)
        response = request.json()

        #print(json.dumps(response, sort_keys=True, indent=4,
        #            ensure_ascii=False, separators=(',', ': ')))

        lang_type=''
        is_translate=False
        for a_resp in response:
            lang_type=a_resp['language']
            # print(f'Detected language is {lang_type}')
            if(lang_type=='zh-Hans'):
                is_translate=True

        # Translate Language 
        path = '/translate?api-version=3.0'
        params = '&to=en'
        constructed_url = base_url + path + params

        if(is_translate):
            request = requests.post(constructed_url, headers=headers, json=body2)
            response = request.json()

            # print (type(response))

            for a_resp in response:
                trans=a_resp['translations']
                for atran in trans:
                    print (atran['text'])
                    trans_content.append(atran['text'])

            #print(json.dumps(response, sort_keys=True, indent=4,
                            #ensure_ascii=False, separators=(',', ': ')))
        else:
            print (f'Detected Language is {lang_type}, didn''t translate it')

    i=0

    print(f"There are {len(trans_content)} translated text")
    for tran_c in trans_content:
        df.iloc[start_index+i,ts_index]=tran_c
        i+=1

    #for tran_c in trans_content:
    #print (df.iloc[0:20,:])

    return df

# ---------------------- Main ----------------------------------- 

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Azure Translation API batch translation')
    parser.add_argument("-s", "--source-filename" , dest="sourceFilename",   help="Soure dataframe in parquet format")
    parser.add_argument("-f", "--column-from" , dest="columnFrom",  help="Column name that need to be translated")
    parser.add_argument("-t", "--column-to" , dest="columnTo",  help="Column name that save translated content")
    parser.add_argument("-k", "--subscription-key" , dest="SKey",  help="Subscription key for Azure tranlation API")
    parser.add_argument("-b", "--batch-size" , dest="batchSize", type=int, help="Batch size for tranlation, maxnium is 100")
    parser.add_argument("-r", "--result-filename" , dest="resultFileName",   help="Result filename to save")

    args = parser.parse_args()
    
    #df=pd.read_parquet("translated_data.parquet")
    df=pd.read_parquet(args.sourceFilename)

    print (df.shape)

    # translated_column_name='translated_descr'
    # source_column_name='Incident description (Title)'
    #translated_column_name='ResolveNotes_EN'
    translated_column_name=args.columnTo
    source_column_name=args.columnFrom
    #source_column_name='Resolve Notes'

    #subscriptionKey='0e85923d2f384d8db22160a0f9dd1007'
    subscriptionKey=args.SKey


    i=0
    batch_size=args.batchSize  # Based on translation API,  Max batch_size is 100, and the total characters are 5000
    df_trans=df#.iloc[1900:2010,:]
    total_records=len(df_trans)
    while i<total_records-1:
        total_char=0
        end_record=i+batch_size-1
        if end_record<total_records-1:
            print (f" Processing {i} ~ {end_record} data ")
        else:  # Last batch 
            print (f" Processing {i} ~ {total_records-1} data ")
            end_record=total_records-1

        df_trans=batch_translate(df_trans,i,end_record,source_column_name,translated_column_name,subscriptionKey )
        i+=batch_size

    print ("Translated result ------------------------   ")
    print (df_trans.head(10))
    #df_trans.to_parquet('translated_data2.parquet')
    df_trans.to_parquet(args.resultFileName)

