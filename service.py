# -*- coding: utf-8 -*-

import sys 
import email
import json
import os
import re
from datetime import datetime
import logging
from html.parser import HTMLParser

import boto3
import requests
import dominate 
from dominate.tags import *
from langdetect import detect

## Thanks to:
# Dincer Kavraal -- dincer(AT)mctdata.com
# https://gist.github.com/dkavraal/356dc60f8f6beb8b5070e891adadab96
##

LI_BASE_URL='https://www.linkedin.com/comm/jobs/view/'

Log = logging.getLogger()
Log.setLevel(logging.ERROR)

FROM_ADDRESS = os.environ['FROM_ADDRESS'] 
EMAIL_DOMAIN = os.environ['EMAIL_DOMAIN']
RE_DOMAIN = re.compile("\@(.*)$")

class DescriptionParser(HTMLParser):
  def __init__(self):
    HTMLParser.__init__(self)
    self.recording_title = False
    self.recording_desc = False
    self.data = {'title':'','description':[]}

  def handle_starttag(self, tag, attributes):
    if tag == 'title':
        self.recording_title = True
    elif tag=='div':
        for name, value in attributes:
            if name == 'class' and value == 'description__text description__text--rich':
                self.recording_desc = True
    else:
        return

  def handle_endtag(self, tag):
    # we only go one deep
    if tag=='div':
        self.recording_desc = False
    if tag=='title':
        self.recording_title = False
    return

  def handle_data(self, data):
    
    if self.recording_title:
        self.data['title']=data
        print('title:',data)
    if self.recording_desc:
       self.data['description'].append(data)


def decode_email(msg_str):
    """ 
    Extract the plain text message body from the email 
    """ 
    p = email.parser.Parser()
    message = p.parsestr(msg_str)
    
    decoded_message = ''
    for part in message.walk():
        charset = part.get_content_charset()
        if part.get_content_type() == 'text/plain':
            decoded_message = part.get_payload()
            
    return decoded_message

def print_with_timestamp(*args):
    print(datetime.utcnow().isoformat(), *args)


def extract_jobs(plain_text_body):
    """
    Extract the title, company, location and job ids contained in message body
    """
    job_ids=set([x.split('/')[-1] for x in re.findall(LI_BASE_URL+r"\d+",plain_text_body)])
    job_ids=list(filter(lambda x: len(x) == 10, job_ids))

    return job_ids


def filter_jobs(job_ids, required_languages=['en']):
    """
    Filter the list of jobs to keep only those where job description is in the list of required languages.
    """
    filtered_jobs = []

    for job_id in job_ids:
        
        job_url = f'{LI_BASE_URL}{job_id}' 
        r = requests.get(job_url)

        if r.ok:
            dp=DescriptionParser()
            dp.feed(r.text)
            title = dp.data.get('title','No Title Found')
            place = title.split(' in ')[1].split(' | ')[0]
            job_title = title.split(' hiring ')[1].split(' in ')[0]
            company = title.split(' hiring')[0]
            try:
                language = detect('\n'.join(dp.data['description']))
            except:
                language = 'unknown'
            if language in required_languages:
                filtered_jobs += [(job_title,company,place, job_url)]
        else:
            # better luck next time, no retries
            continue

    return filtered_jobs


def build_reply(filtered_jobs,subject):

    # build a message in plain text and html to send back
    datefmt = datetime.now().strftime("%d %B %Y")
    salutation=f'These are your filtered job alerts for {datefmt}'

    plaintext_body = f'Job Alerts\n-------------\n{salutation}\n\n'
    plaintext_body += '\r\n'.join([f"{i+1}. {', '.join(j)}" for i,j in enumerate(filtered_jobs)])
    plaintext_body += '\nThanks for using li-filter'

    htmldoc = dominate.document(title='Job Alerts')
    htmldoc.add(body()).add(div(id='content'))

    # put the jobs in a table with links.
    clrs=['#ffffff','#EEF7FA']
    with htmldoc.body:
        h1('Job Alerts')
        p(subject+f" on {datefmt}")

        head_style = "background-color:#005b96;color:#ffffff;border:0;font-weight:bold"
        cell_style = f"border:0;padding:0.25em"
        
        with table(style='border-collapse: collapse;').add(thead([td('',style=head_style),
                                td('Job Title',style=head_style), 
                                td('Company',style=head_style), 
                                td('Place',style=head_style), 
                                td('Id',style=head_style)])).add(tbody()):
            for i,job in enumerate(filtered_jobs):

                row_style = f"background-color:{clrs[i%2]};border:0"

                trow = tr(style=row_style)
                trow += td(f'{i+1}.',style=cell_style)
                trow += td(job[0],style=cell_style)
                trow += td(job[1],style=cell_style)
                trow += td(job[2],style=cell_style)
                trow += td(a(job[3].split('/')[-1],href=job[3]),style=cell_style)
                
    return (plaintext_body, htmldoc)


def handler(event, context):
    
    Log.debug(json.dumps(event, indent=4))

    ses_notification = event['Records'][0]['Sns']
    message_id = ses_notification['MessageId']
    message = json.loads(ses_notification["Message"])
    receipt = message['receipt']
    sender = message['mail']['source']
    subject = message['mail']['commonHeaders']['subject']
    sender_domain = (RE_DOMAIN.findall(sender) or [""])[0]
    
    print_with_timestamp('Accepting message:', message_id)

    # now distribute to list:
    action = receipt['action']
    if (action['type'] != "S3"):
        Log.exception("Mail body is not saved to S3. Or I have done something wrong.")
        return None

    try:
        ses_client = boto3.client('ses')
        s3_client = boto3.resource('s3')
        mail_obj = s3_client.Object(action['bucketName'], action['objectKey'])

        plain_text_body = decode_email(mail_obj.get()["Body"].read().decode("utf-8"))
        filtered_jobs = filter_jobs(extract_jobs(plain_text_body))
        textresp, htmlresp = build_reply(filtered_jobs, subject)
        resp_subject = f'Hey: {len(filtered_jobs)} new jobs {subject.split("new jobs")[-1]}'
        resp_body = {
                        'Text': {
                            'Data': textresp,
                        },
                        'Html': {
                            'Data': htmlresp.render(pretty=False),
                        }
        }
        print(resp_body)
        try:
        
            response = ses_client.send_email(
                Source=FROM_ADDRESS,
                Destination={
                    'ToAddresses': [sender],
                },  
                Message={
                    'Subject': { 'Data': resp_subject },
                    'Body': resp_body
                },  
            )
        except Exception as e1:
            print_with_timestamp(e1)
            raise e1
    
    except Exception as e2:
        print_with_timestamp(e2)
        raise e2

    return None


