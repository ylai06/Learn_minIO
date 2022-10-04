# %%
# Include packages
from minio import Minio
from minio.error import InvalidResponseError
import os

# %%
# global variables
# connect Minio (bucket = senser folder, object = file)
minioClient = Minio('x.x.x.x:9000', # 文件服務地址 本機ip+port:9000
                    access_key='minioadmin', # 用戶名
                    secret_key='minioadmin', # 密碼
                    secure=False) # 設置': True代表啟用HTTPS 

# connect AWS S3
# minioClient = Minio('s3.amazonaws.com',
#                  access_key= 'YOUR_ACCESSKEYID',
#                  secret_key= 'YOUR_SECRETACCESSKEY',
#                  secure=False)

# %%
def getFiles(path: str) -> list:
  """
    Get all local file names from local folder

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    files : dict 

  """
  try:
    files = os.listdir(path) 
  except OSError as e:
    print(e)
  return files

# %%
# Create bucket 
def create_bucket(bucket_name: str) -> bool:
  global minioClient
  try:
    if minioClient.bucket_exists(bucket_name): 
      print("bucket <{}> already exist".format(bucket_name))
      return False
    else:
        minioClient.make_bucket(bucket_name)
        print("bucket <{}> create success".format(bucket_name))
  except InvalidResponseError as err:
      print(err)
  return True

# %%
# Get all bucket 
def get_bucket_list() -> list:
  global minioClient
  get_bucket=[]
  try:
    buckets = minioClient.list_buckets()
    for bucket in buckets:
      print(bucket.name, bucket.creation_date)
      get_bucket.append({
        'bucket_name': bucket.name,
        'create_time': bucket.creation_date
      })
  except InvalidResponseError as err:
    print(err)
  return get_bucket

# %%
# Delete bucket 
def get_remove_bucket(bucket_name: str) -> bool:
  global minioClient
  try:
    minioClient.remove_bucket(bucket_name)
    print("bucket <{}> delete success".format(bucket_name))
  except InvalidResponseError as err:
    print(err)
    return False
  return True
  
# %%
# Upload local file to Minio Bucket 
'''
  單個對象的最大大小限制在5TB。 
  put_object在對像大於5MiB時，自動使用multiple parts方式上傳。 
  這樣，當上傳失敗時，客戶端只需要上傳未成功的部分即可（類似斷點上傳）。 
  上傳的對象使用MD5SUM簽名進行完整性驗證??
'''
def upload_object(bucket_name: str, object_name: str, file_path: str) -> bool:
  '''
  params:
    bucket_name: bucket名稱 ex. test1
    object_name: object名稱 ex. oct.npz
    file_path: 本機文件的路徑+文件名稱 ex. C:\Minio\test1\20220909\oct.npz
  '''
  global minioClient
  try:
    minioClient.fput_object(bucket_name, object_name, file_path, content_type='application/x-npz')
  except InvalidResponseError as err:
    print(err)
    return False
  return True

# %%
# Upload: local folder to Bucket v
def upload_folder(local_path: str, bucket_name: str, minio_path: str=''):
  '''
  params:
    local_path: 本機上傳文件夾位置 ex. C:\Minio\test1
    bucket_name: bucket名稱 ex. test1
    minio_path: 保存在minio的路徑,自動添加在bucket下 ex. test -> test1/test 
                default: '' (與local_path相同)
  '''
  global minioClient
  files = getFiles(local_path)
  for local_file in files:
      file_path = os.sep.join([local_path, local_file]) 
      remote_path = os.sep.join([minio_path, local_file])
      if not os.path.isfile(file_path):
          upload_folder(file_path, bucket_name, remote_path)
      else:
          sta = upload_object(bucket_name, remote_path, file_path)
          if(sta):
            print('upload success, folder path:', local_path)
          else:
            print('upload failed')

# %%
# Get objects under the Bucket v
def get_object_list(bucket_name: str, prefix: str=None) -> list:
  '''
  params:
    bucket_name: bucket名稱 ex. test1
    prefix: folder path ex. 20220909  
            default: None (get all object)
  '''
  global minioClient
  try:
    obj_list = minioClient.list_objects(bucket_name, prefix, recursive=True)
    get_list = []
    for obj in obj_list:
      get_list.append({
        'bucket_name': obj.bucket_name, 
        'object_name': obj.object_name, 
        'is_dir': obj.is_dir,
        'last_modified': obj.last_modified,
        'etag': obj.etag, 
        'size': obj.size, 
        'content_type': obj.content_type
      })
  except InvalidResponseError as err:
    print(err)
  return get_list

# %%
# Download object to local
def download_object(bucket_name: str, object_name: str, local_file_path: str) -> dict:
  '''
  params:
    bucket_name: bucket名稱 ex. test1
    object_name: object名稱 ex. 20220909/oct.npz
    local_folder_path: 本機下載位置路徑 ex. C:\Minio\20220909 
  '''
  global minioClient
  get_obj = {}
  try:
    obj = minioClient.fget_object(bucket_name, object_name, os.sep.join([local_file_path, object_name]))
    get_obj = {
      # 'metadata:': obj.metadata,
      'last_modified': obj.last_modified,
      'etag': obj.etag, 
      'size': obj.size, 
      'content_type': obj.content_type
    }
  except InvalidResponseError as err:
    print(err)
  print('download successed, save_local_path:', local_file_path)
  return get_obj

# %%
# download all files in bucket, and saved to local folder 
def download_folder(bucket_name: str, local_folder_path: str, folder_name: str=None) -> dict:
  '''
  params:
    bucket_name: bucket名稱 ex. test1
    local_folder_path: 本機下載位置路徑(會自動建立對應文件夾) ex. C:\Minio\20220909 
    folder_name: 下載文件夾 ex. 20220909
                 default: None
  '''
  global minioClient
  object_to_download = get_object_list(bucket_name, folder_name)
  del_obj = []
  for obj in object_to_download:
    del_obj.append(download_object(bucket_name, obj['object_name'], local_folder_path))
  return del_obj

# %%
# delete object
def del_object(bucket_name: str, object_name: str) -> bool:
  '''
  params:
    sensor_name: 刪除bucket名稱 ex. test1
    object_name: 文件路徑+名稱 ex. 20220909/oct.npz
  '''
  global minioClient
  try:
    minioClient.remove_object(bucket_name, object_name)
    print('delete object <{}> successed'.format(object_name))
  except InvalidResponseError as err:
    print(err)
    return False
  return True

# %%
# Delete objects in bucket folder
def del_folder(bucket_name: str, folder_name: str=None):
  '''
  params:
    bucket_name: 刪除bucket名稱 ex. test1
    folder_name: 文件路徑 ex. 20220909
                 default: None
  ''' 
  global minioClient
  bucket = get_object_list(bucket_name, folder_name)
  for obj in bucket:
    obj_name = obj['object_name'] 
    del_object(bucket_name, obj_name)   

if __name__ == '__main__':
  # 新增bucket
  create_bucket('mybucket')
  create_bucket('20220910')
  create_bucket('test1')
  
  # 查看bucket/object
  # print(get_bucket_list())
  # print(get_object_list('mybucket')) 

  # 上傳
  # path = os.path.abspath('./demoMinio/temp/test1')
  # print(upload_object('test', '11.PNG', os.sep.join([path, '11.PNG'])))
  # upload_folder(path, 'test')
  
  # 下載
  # d_path = os.path.abspath('./demoMinio/temp/test2')
  # download_object('mybucket', 'img/pigeon.PNG', d_path)
  # download_folder('mybucket', d_path)
 
  # 刪除
  # del_object('mybucket', 'pigeon.PNG')
  # del_folder('mybucket', '20220912')
  # del_folder('mybucket')
  get_remove_bucket('20220910')
  get_remove_bucket('mybucket')

  print('end')
