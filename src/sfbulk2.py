import requests

DEFAULT_API_VERSION = "v47.0"


class SFBulk2(object):
  def __init__(self, access_token=None, instance_url=None, api_version=DEFAULT_API_VERSION):
    if not access_token or not instance_url:
      raise RuntimeError("Must provide access_token and instance_url!")
    print('api_version: {}'.format(api_version))
    self.access_token = access_token
    self.instance_url = instance_url
    self.api_version = api_version
  
  def create_job(self, operation=None, obj=None, contentType='CSV', lineEnding='LF'):
    if not operation or not obj:
      raise RuntimeError("Must provide operation and obj!")
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json'
    }
    
    print ('headers: {}'. format(headers) )
    body = {
      "object" : obj,
      "contentType" : contentType,
      "operation" : operation,
      "lineEnding": lineEnding 
    }
    print ('body: {}'. format(body) )
    uri = '{}/services/data/{}/jobs/ingest/'.format(self.instance_url, self.api_version)
    print ('uri: ' + uri)
    response = requests.post(uri, headers=headers, json=body)
    job_id = response.json()['id']
    print ('job_id: {}'.format(job_id))
    return job_id
  
  def get_job_status(self, job_id=None, optype=None):
    if not job_id:
      raise RuntimeError("Must provide job_id!")
    if not optype:
      raise RuntimeError("Must provide optype ['ingest', 'query']!")
    uri = '{}/services/data/{}/jobs/{}/{}'.format(self.instance_url, self.api_version,optype, job_id)
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json'
    }
    print ('uri: ' + uri)
    post_res = requests.get(uri, headers=headers)
    return post_res
  
  def put_data(self, contentUrl=None, data=None):
    if not contentUrl or not data:
      raise RuntimeError("Must provide contentUrl and data!")
    put_uri = '{}/{}'.format(self.instance_url, contentUrl)
    put_headers = {
    'Authorization': 'Bearer ' + self.access_token,
    'Content-Type': 'text/csv',
    'Accept': 'application/json'
    }
    print (put_uri)
    put_response = requests.put(put_uri, headers=put_headers, data=data)
    return put_response

  def patch_state(self, job_id=None, state=None):
    # You can only delete a job if its state is JobComplete, Aborted, or Failed.
    if not job_id:
      raise RuntimeError("Must provide job_id!")
    if not state:
      raise RuntimeError("Must provide state: ['UpdateComplete']")
    patch_uri = '{}/services/data/{}/jobs/ingest/{}'.format(self.instance_url, self.api_version,job_id)
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json'
    }
    print ('uri: ' + patch_uri)
    patch_body = {"state" : state }
    patch_res = requests.patch(patch_uri, json=patch_body, headers=headers)
    return patch_res
  
  def get_failure_status(self, job_id=None):
    if not job_id:
      raise RuntimeError("Must provide job_id!")
    jobs_failure_uri = '{}/services/data/{}/jobs/ingest/{}/failedResults/'.format(self.instance_url, self.api_version,job_id)
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json'
    }
    failure_res = requests.get(jobs_failure_uri, headers=headers)
    return failure_res

  ### --------- QUERY -----------
  def create_query_job(self, query=None):
    operation = 'query'
    if not query :
      raise RuntimeError("Must provide SOQL query!")
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json'
    }
    body = {
      "operation" : operation,
      "query": query,
      "contentType" : "CSV",
      "columnDelimiter" : "COMMA",
      "lineEnding" : "LF" 
    }


    print ('body: {}'. format(body) )

    uri = '{}/services/data/{}/jobs/query'.format(self.instance_url, self.api_version)
    print ('uri: ' + uri)
    response = requests.post(uri, headers=headers, json=body)
    job_id = response.json()['id']
    print ('job_id: {}'.format(job_id))
    return job_id
  

  def get_all_query_jobs(self):
    results_uri = '{}/services/data/{}/jobs/query'.format(self.instance_url, self.api_version)
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'text/csv'
    }
    query_res = requests.get(results_uri, headers=headers)
    return query_res
    
  def get_query_results(self, job_id=None):
    if not job_id:
      raise RuntimeError("Must provide job_id!")
    results_uri = '{}/services/data/{}/jobs/query/{}/results'.format(self.instance_url, self.api_version,job_id)
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'text/csv'
    }
    #print (headers)
    #print ('results_uri: ' + results_uri)

    query_res = requests.get(results_uri, headers=headers)
    return query_res


  def abort_query_job(self, job_id=None):
    if not job_id:
      raise RuntimeError("Must provide job_id!")
    abort_query_uri = '{}/services/data/{}/jobs/query/{}/'.format(self.instance_url, self.api_version,job_id)
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'text/csv'
    }

    abort_res = requests.patch(abort_query_uri, json= {'state': 'Aborted'}, headers=headers)
    return abort_res
  

  def delete_query_job(self, job_id=None):
    if not job_id:
      raise RuntimeError("Must provide job_id!")
    delete_query_uri = '{}/services/data/{}/jobs/query/{}/'.format(self.instance_url, self.api_version,job_id)
    headers = {
      'Authorization': 'Bearer ' + self.access_token,
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'text/csv'
    }

    delete_res = requests.delete(delete_query_uri, headers=headers)
    return detelete_res
  
  def ingest_multipart (self, operation=None, obj= None, data=None):
    if not operation or not obj or not data:
      raise RuntimeError("Must provide operation, obj and data")
    uri = '{}/services/data/{}/jobs/ingest/'.format(self.instance_url, self.api_version)
    headers =  {
        'Authorization': 'Bearer ' + self.access_token,
        "Content-Type": "multipart/form-data; boundary=BOUNDARY" ,
        "Accept": "application/json"
    }
    body = '''
--BOUNDARY
Content-Type: application/json
Content-Disposition: form-data; name="job"

{
  "object":"%s",
  "contentType":"CSV",
  "operation": "%s",
  "lineEnding": "LF"
}

--BOUNDARY
Content-Type: text/csv
Content-Disposition: form-data; name="content"; filename="content"
%s
--BOUNDARY--
''' % ( obj, operation, data)
    print (body)
    return requests.post(uri, headers=headers, data=body)


