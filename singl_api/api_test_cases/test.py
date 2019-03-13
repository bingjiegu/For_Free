# coding:utf-8
from tkinter import *
from new_api_cases.check_result import CheckResult
from api_test_cases.get_execution_output_json import GetCheckoutDataSet

root = Tk()
w1 = Label(root, text=r'你可以在该页面进行测试用例的新增，编辑，执行等操作')

w1.config(fg='black',font=('隶书', 16, 'bold'), pady=30)  # text的字体颜色，和背景颜色
w1.place(x=210, y=0)


b1 = Button(text='执行API测试用例', command=CheckResult().deal_result)
b2 = Button(text='执行Flow测试用例', command=GetCheckoutDataSet().get_json)
b1.config(font=('隶书', 15, 'bold'),padx=5,pady=5)
b2.config(font=('宋体', 15, 'bold'),padx=5,pady=5)
b1.place(x=250, y=180)
b2.place(x=550, y=180)
root.config(width=1000, height=500)
mainloop()
