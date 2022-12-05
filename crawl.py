# -*- coding: utf-8 -*-
import pandas as pd
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

data_url = 'https://www.fahasa.com/fahasa_catalog/product/loadCatalog?category_id='
url_product_list = 'https://www.fahasa.com/fahasa_catalog/product/loadCatalog?category_id={}&currentPage=1&limit={}&order=num_orders&series_type=0&fbclid=IwAR0rrA5ALIcS4Jq5koZ2mPkQ8zInC7Yp-9TWCYzAxT9b3nwT79pMePv1-ds'


t = time.time()

#lấy dữ liệu từ api
def get_data_from_api(url,att):
    headers = requests.utils.default_headers()
    headers.update(
        {
            'User-Agent': 'My User Agent 1.0',
        }
    )
    response = requests.get(url, headers=headers)
    json = response.json()
    if(type(json[att])==list):
        df = pd.DataFrame(json[att])    
    else:
        df = json[att]
    return df

def get_data_from_api2(url):
    headers = requests.utils.default_headers()
    headers.update(
        {
            'User-Agent': 'My User Agent 1.0',
        }
    )
    response = requests.get(url, headers=headers)
    return response.json()

# #check exists
# def is_exists(url):
#     res = requests.get(url)
#     return json.loads(res.text)

# #insert data
# def insert(table_name, data):
#     url = api_url + 'insert?table_name='+table_name
#     res = requests.post(url, data=json.dumps(data))
#     print(res.text)
#     return json.loads(res.text)

# def insert_list(table_name, data):
#     url = api_url + 'insert-list?table_name='+table_name
#     res = requests.post(url, data=json.dumps(data))
#     print(res.text)
#     return json.loads(res.text)

# #get request
# def getData(url):
#     res = requests.get(url)
#     return json.loads(res.text)

#bat dong bo
def runner(data, func, max_workers = 1):
    # print(data)
    threads= []
    with ThreadPoolExecutor(max_workers) as executor:
        for i in data:
            threads.append(executor.submit(func, i))
        for task in as_completed(threads):
            print(task.result()) 

# #crawl categories
# old_categories = getData(api_url + 'get-all?table_name=category')
# new_categories = []
# all_cate_ids = [2]

# def get_cate(data):
#     _id, parent_id = data
#     df = get_data_from_api(data_url + str(_id), 'children_categories')
#     if(not 'id' in df.columns):
#         return  
    
#     new_cate = get_data_from_api(data_url + str(_id),'category')
#     new_cate['parent_category'] = _id
#     new_cate['id'] = int(new_cate['id'])
#     new_cate in old_categories and new_categories.append(new_cate)
#     print(len(all_cate_ids))
#     cur_cate_ids = df['id'].values.tolist()
#     new_cate_ids = [i for i in cur_cate_ids if i not in all_cate_ids]
    
#     if(new_cate_ids == []):
#         return 
#     all_cate_ids.extend(new_cate_ids)
#     next_cate = [[i, _id] for i in new_cate_ids]
#     runner(next_cate, get_cate)
        
# get_cate([2, 2])    
# #dat lenh insert o day
# print(all_cate_ids)

# # Crawl attributes
# old_attributes = getData(api_url + 'get-all?table_name=attribute')
# cur_attributes = get_data_from_api(data_url+"2",'attributes')[['id', 'code', 'label']]
# cur_attributes['id'] = pd.to_numeric(cur_attributes['id'])
# new_attributes = [i for i in cur_attributes.to_dict('records') if i not in old_attributes]
# print('new attributes')
# print(new_attributes)

# # #Crawl options
# old_options = getData(api_url + 'get-all?table_name=options')
# new_options = []

# def filter_options(row):
#     row = row._asdict()
#     options = pd.DataFrame(row['options'])[['id', 'label']]
#     options['attr_id'] = int(row['id'])
#     options['id'] = pd.to_numeric(options['id'])
    
#     cur_options = options.to_dict('records')
#     data = [i for i in cur_options if i not in new_options]
#     new_options.extend(data)

# def get_options(_id):
#     df = get_data_from_api(data_url + str(_id),'attributes')
#     if('id' and 'options' not in df.columns):
#         return
#     for index, row in df[['id', 'options']].iterrows():
#         options = pd.DataFrame(row['options'])[['id', 'label']]
#         options['attr_id'] = int(row['id'])
#         options['id'] = pd.to_numeric(options['id'])
        
#         cur_options = options.to_dict('records')
#         data = [i for i in cur_options if i not in new_options and i not in old_options]
#         new_options.extend(data)
#     return data
    
        
# cate_ids = getData(api_url + "cate-childlest-list")
# runner(cate_ids, get_options)
# #dat lenh insert o day
# print('new options')
# print(new_options)

# #crawl product_options
# old_product_option = getData(api_url + 'get-all?table_name=product_option')
# new_product_option = []
# attr_codes = getData(api_url + "attribute-codes")
# option_ids = sum([getData(api_url + "option?code="+code) for code in attr_codes], [])

# def crawl_product_option(option):
#     url = data_url+'{}&filters%5B{}%5D={}'.format(2, option['code'], option['id'])  
#     total = get_data_from_api(url,'total_products')
#     print(url)
#     products = get_data_from_api(url + '&limit=' + str(total),'product_list')
#     if(not 'product_id' in products.columns):
#         return
#     product_ids = products[['product_id']]
#     product_ids['product_id'] = pd.to_numeric(product_ids['product_id'])
#     product_ids['option_id']  = option['id']
    
#     cur_product_option = product_ids.to_dict('records')
#     data = [i for i in cur_product_option if i not in old_product_option]
#     new_product_option.extend(data)
#     print(len(new_product_option))
            
# runner(option_ids, crawl_product_option)
# #dat lenh insert o day
# print ("done in ", time.time()- t)
        

# #----------------------Product_list-------------------------------

# #lấy tổng sản phẩm từ một id danh mục con
# def get_total_product(category_id):
#     url = data_url + category_id
#     att = 'total_products'
#     headers = requests.utils.default_headers()
#     headers.update(
#         {
#             'User-Agent': 'My User Agent 1.0',
#         }
#     )
#     response = requests.get(url, headers=headers)
#     json = response.json()
#     total_product = json[att]
    
#     return total_product

# # Lấy danh sách sản phẩm về
# def get_data_product(category_id):
#     att = 'product_list'
#     total_product = get_total_product(category_id)
#     url = url_product_list.format(category_id,total_product)
#     df = get_data_from_api(url,att)
#     df.fillna(value=0, inplace=True)
#     df['category_id'] = category_id
#     df['product_price'] =  [int(str(p).replace('.', '')) for p in df['product_price']]
#     df['product_finalprice'] =  [int(str(p).replace('.', '')) for p in df['product_finalprice']]
#     df.rename(columns = {'product_id':'id'}, inplace=True)
#     return df

# cate_child_id = json.loads(requests.get(api_url + "cate-childlest-list").text)

# for index in cate_child_id:
#     product_list = get_data_product(str(index))
#     product_list_new = [insert('product', row.to_dict()) for index, row in product_list.iterrows() if not is_exists(check_url1.format('product', row['id']))]
