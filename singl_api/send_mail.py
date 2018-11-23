import smtplib
import os.path as pth
import time
from email import encoders
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.utils import parseaddr, formataddr
from email.mime.base import MIMEBase
from basic_info.setting import email_to

def sendEmail(content, title, from_name, from_address, to_address, serverport, serverip, username, password):
    # msg = MIMEText(content, _subtype='html', _charset='utf-8')
    # msg = MIMEText(content, 'html', 'utf-8')
    # 邮件对象:
    msg = MIMEMultipart()
    msg['Subject'] = Header(title, 'utf-8')
    # 这里的to_address只用于显示，必须是一个string
    msg['To'] = ','.join(to_address)
    msg['From'] = from_name

    # 邮件正文是MIMEText:
    msg.attach(MIMEText(content, 'html', 'utf-8'))

    # 添加附件就是加上一个MIMEBase，从本地读取一个图片:
    with open('E:\Reports\Test_report.html', 'rb') as f:
        # 设置附件的MIME和文件名，这里是png类型:
        mime = MIMEBase('report', 'html', filename='Test_Report.html')
        # 加上必要的头信息:
        mime.add_header('Content-Disposition', 'attachment', filename='Test_Report.html')
        mime.add_header('Content-ID', '<0>')
        mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        mime.set_payload(f.read())
        # 用Base64编码:
        encoders.encode_base64(mime)
        # 添加到MIMEMultipart:
        msg.attach(mime)

    try:
        s = smtplib.SMTP_SSL(serverip, serverport)
        s.login(username, password)
        # 这里的to_address是真正需要发送的到的mail邮箱地址需要的是一个list
        s.sendmail(from_address, to_address, msg.as_string())
        print('%s----发送邮件成功' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    except Exception as err:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print(err)
        #HEFEN_D = pth.abspath(pth.dirname(__file__))

def main2():
    # from run import filename
    TO = [email_to["gubingjie"], email_to["daming"]]
    config = {
    "from": "ruifan_test@163.com",
    "from_name": 'bingjie',
    "to": TO,
    "serverip": "smtp.163.com",
    "serverport": "465",
    "username": "ruifan_test@163.com",
    "password": "ruifantest2018"  # QQ邮箱的SMTP授权码
             }

    title = "自动化测试报告"
    f = open("E:\Reports\Test_Report.html", 'rb')
    mail_body = f.read()
    f.close()
    sendEmail(mail_body, title, config['from_name'], config['from'], config['to'], config['serverport'], config['serverip'], config['username'], config['password'])

