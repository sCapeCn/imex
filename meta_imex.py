import os
import sys
import json
from datetime import datetime
from io import BytesIO

import django
import sqlalchemy
import xlwt
import xlrd
from xlrd import xldate_as_datetime, xldate_as_tuple

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR+'/../')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'odp.settings')
django.setup()

from django.db.models import Q
from odp.models.system.bsobject import BsModel
from odp.models.form.formobj import IdWorker

"""

导入导出二次整理

"""

DBTYPES = {
		'mysql': 'pymysql',
		'postgresql': 'psycopg2',
		'oracle': 'cx_oralce',
		'sqlite': 'sqlite',
		'sqlserver': 'sqlserver'
		}


class OdpException(Exception):
	pass


class MetaBase:
	"""
	导入导出基类
	"""
	def __init__(self, connector):
		"""
		@params: 数据库连接初始化参数
			connector: {
				'user': 'odp',
				'password': '12345678',
				'ip': '127.0.0.1',
				'port': '5432',
				'db': 'odp'
			}
		"""
		self.connector = connector
		self.export_data = []
		self.import_data = None
	
	def init_none():
		"""空函数"""
		
		return None

	def _init_session(self, db_type='postgresql'):
		"""
			初始化数据库查询session, 初始化线程池
			connector: 连接数据库的配置, dict类型
		"""
		# 数据库类型与驱动映射
		INIT_DB = {
				'mysql': self._mysql,
				'postgresql': self._psql,
				'oracle': self._oracle,
				'sqlite': self._sqlite,
				#'sqlserver': self._sqlserver,
				} 

		if db_type not in INIT_DB.keys():
			raise OdpException('当前不支持该数据库类型')

		conn = INIT_DB.get(db_type, self.init_none)()
		return conn

	def _oracle(self):
		"""
			oracle 驱动初始化
		"""

		engine = create_engine('oracle://{user}:{password}@{host}:{port}/{db}'.format(**self.connector),
					#user=self.conf.get('user'),
					#password=self.conf.get('password'),
					#host=self.conf.get('host'),
					#port=self.conf.get('port'),
					#db=self.conf.get('db')),
					echo=True)
		DB_Session = sessionmaker(bind=engine)
		session = DB_Session()
		return session

	def _sqlite(self):
		"""
			sqlite 驱动初始化
		"""

		engine = create_engine('sqlite:///{db}'.format(db=self.connector.get('db')), echo=True)
		self.session = engine
		return self.session
	
	def _psql(self):

		engine = create_engine('postgresql+psycopg2://{user}:{password}@{ip}:{port}/{db}'.format(**self.connector),  # 用户名:密码@localhost:端口/数据库名
				max_overflow=0,  # 超过连接池大小外最多创建的连
				pool_size=150,	# 连接池大小
				pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
				pool_recycle=-1)  # 多久之后对线程池中的线程进行一次连接的回收（重置）
		DBsession = sessionmaker(bind=engine)
		return scoped_session(DBsession) # 线程安全

	def _mysql(self):
		"""
			mysql 驱动初始化
		"""
		engine=create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
					**default
					),
					echo=True)

		DB_Session = sessionmaker(bind=engine)
		self.session = DB_Session()
		return self.session


	def _write_xls(self, sheet_name='sheet1', titles=[], data=[]):
		
		""" 
			将查询的sql写入到excel格式中，并放到缓存中
			titles: ['a', 'b', 'c']
			data: [
				{'a': 1, 'b': 2, 'c': 3}
			]
		"""
		workbook=xlwt.Workbook()
		#m_font = xlwt.Font()
		#style = xlwt.XFStyle()
		#style.alignment.wrap = 1

		style = xlwt.XFStyle() # 声明样式
		alignment = xlwt.Alignment()
		alignment.wrap = 1
		alignment.horz = 1 # 1.居左，2居中
		style.alignment = alignment # 样式加载对齐方式

		sheet=workbook.add_sheet(sheet_name)
		if titles:
			for i,v in enumerate(titles):
				sheet.write(0, i, v)
			row_start = 1
		else:
			row_start = 0

		for x, _d in enumerate(data):
			y = 0
			for k, v in _d.items():
				sheet.write(x+row_start, y, v, style=style)
				y+=1

		sio=BytesIO()
		workbook.save(sio)
		sio.seek(0)
		return	sio.getvalue()

	def _read_cls(self, xlf='20210422.xlsx',sheet_idx=0, sheet_name='', merged=False):
		"""
		读取excel文件，返回字典列表
		#table = data.sheet_by_index(sheet_indx)) #通过索引顺序获取
		#table = data.sheet_by_name(sheet_name)#通过名称获取
		#names = data.sheet_names()    #返回book中所有工作表的名字
		#data.sheet_loaded(sheet_name or indx)	 # 检查某个sheet是否导入完毕
		
		xlf: excel文件
		sheet_idx: sheet页索引
		sheet_name: sheet页名称

		"""
		data = xlrd.open_workbook(xlf)
		if sheet_name:
			table = data.sheet_by_name(sheet_name)
		else:
			table = data.sheets()[sheet_idx]		  #通过索引顺序获取
		nrows = table.nrows  #获取该sheet中的有效行数
		datas = []

		if merged:
			# 不支持
			merged = table.merged_cells # 合并的单元格
		
		for i in range(nrows):
			ncols = table.ncols #获取列表的有效列数
			col_dict = {}
			for j in range(ncols):
				the_cell = table.cell(i,j)	#返回由该列中所有的单元格对象组成的列表
				col_dict[j] = the_cell.value
			datas.append(col_dict)
			#return datas, merged
		return datas

	def date_compatible(self, date_param):
		"""
			把文件中特殊格式的日期格式转换为yyyy-mm-dd格式
		"""

		if type(date_param) == float:
			try:
				date_param = datetime(*xldate_as_tuple(date_param, 0)).strftime('%Y-%m-%d %H:%M:%S') if date_param else None
				return date_param
			except:
				return date_param
		else:
			if '年' in date_param:
				date_param = date_param.replace('年', '-')
			if '月' in date_param:
				date_param = date_param.replace('月', '-')
			if '日' in date_param:
				date_param = date_param.replace('日', '')
			if not date_param:
				return ''
			if '00:00:00' not in date_param:
				date_param = date_param + ' 00:00:00'
		return date_param
	
	def _sql_query(self, show_fields, sql):
		"""
			执行原生sql
			show_fields: 查询的字段
			sql: 查询sql
		"""
		
		session = self._init_session()
		data = session.execute(sql)
		temp_data = []
		for _d in data:
			temp_d = []
			for i in _d:
				if i is None :
					temp_d.append(0)
				else:
					temp_d.append(i)
			temp_data.append(dict(zip(show_fields, list(temp_d))))
		if not temp_data:
			temp_ = {}
			for s in show_fields:
				temp_[s] = ''
			temp_data.append(temp_)
		session.remove()
		self.export_data = temp_data
		return temp_data

	def _model_query(self, show_fields, model=None, filters='delflag=false', istemp=False, comp=0):
		"""
			show_fields: 查询的字段
		"""
		
		mod_obj = BsModel.GetModel(comp, model)
		filter_list = filters.split(' and ')
		qo = Q()
		for f in filter_list:
			field = f.split('=')[0].strip()
			value = f.split('=')[1].strip()
			if value in ['false', 'true']:
				value = value.capitalize()
			qo.children.append((field, value))

		if istemp:
			data = mod_obj.objects.filter(qo).values()
			if data:
				data = data[0]
		else:
			data = mod_obj.objects.filter(qo).values(*show_fields)
			
		return data


class ZBusiness(MetaBase):
	def __init__(self, connector):
		super(ZBusiness, self).__init__(connector)

	def exdb(self, fields, sheet_name='sheet1', sql='', model='', filters='delflag=false', istemp=False, query_type='orgin', **kwargs):
		"""
			导出接口,
			fields: {
				'field': 'field_name',
			}
			sql: 查询的sql
			model: 数据库表模型
			filters: 筛选条件
			sheet_name: excel的sheet页名称
			kwargs: 中存放一些业务相关的参数
		"""
	
		field_names = [] # excel中的title列表
		select_fields = [] # 数据库中的字段列表
		for k, v in fields.items():
			# 防止乱序, 使一一对应
			select_fields.append(k) 
			field_names.append(v)
		if istemp:
			# 导出一条完整数据作为模板
			if query_type == 'orm':
				if not model:
					raise Exception('参数格式错误')
				data = self._model_query(select_fields, model, filters, istemp)
			elif query_type == 'orgin':
				data = self._sql_query(select_fields, sql, istemp)
		else:
			# 默认导出
			if query_type == 'orgin':
				data = self._sql_query(select_fields, sql) # 查询数据结果
			else:
				if not model and not filters:
					raise Exception('参数格式错误')
				data = self._model_query(select_fields, model, filters)
		temp_data = []
		for _d in data:
			for k, v in _d.items():
				if type(v) is datetime:
					# 处理时间类型，在json序列化时datetime不被解析
					_d[k] = datetime.strftime(v, "%Y-%m-%d") if v else None
			temp_data.append(_d)
		res = self._write_xls(sheet_name, titles=field_names, data=temp_data)
		return res

	def imdb(self, file_path, model='', titleline=1, startline = 2, fieldmaps=None, creator=None, comp=0, **kwargs):
		"""
		从excel导入到数据库中
		file_path: excel文件路径
		comp: 公司id 默认0
		model: 导入的模型
		startline: 数据开始的坐标
		fieldmps: 配置项，支持文件，json，字典等格式
		creator: 数据的创建人
		kwargs: 其他待定参数
			typedict: 类型字典，指定excel文件中各个字段的类型
			additional: 值字典， excel文件中值的字典
		"""
		typedict = kwargs.get('typedict') # 类型字典，指定excel文件中各个字段的类型
		if fieldmaps:
			# 指定了字段列表
			if type(fieldmaps) is dict:
				columns = fieldmaps
			elif type(fieldmaps) is str:
				if os.path.isfile(fieldmaps):
					# 文件
					columns = json.load(fieldmaps)
				else:
					# json字符串
					columns = json.loads(fieldmaps)
		else:
			# 未指定字段列表
			# 如果没有指定字段映射， 就要考虑文件中的中文标题名和数据库中的字段一一映射,
			# 该情景实现后会非常危险
			raise Exception('需要指定字段映射关系')
				
		try:
			datas = self._read_cls(file_path)
			temp_columns = {} # {'city': 0}
			if len(datas) == 1:
				# 文件中有且只有头部标题
				return {'code': 0, 'msg': 'success'}

			for k,v in datas[titleline].items():
				# 遍历excel 文件头部
				if v:
					temp_columns[k] = columns.get(v)

			mod_obj = BsModel.GetModel(comp, model)
			for _dt in datas[startline:]:
				# 数据开始的位置，默认为2
				new_obj = mod_obj()
				new_id = IdWorker(0, 1).get_id()
				new_obj.id = new_id
				temp_dict = {}
				for k, v in _dt.items():
					field_name = temp_columns.get(k)
					if typedict.get(k) == 'int':
						v = int(v)
					elif typedict.get(k) == 'string':
						v = str(v)
					elif typedict.get(k) == 'float':
						v = float(v)
					elif typedict.get(k) == 'date':
						v = self.date_compatible(v)
					elif typedict.get(k) == 'additional':
						v = additional.get(v)
					else:
						pass
					if field_name:
						temp_dict[field_name] = v
				new_obj.__dict__.update(temp_dict)
				if creator:
					new_obj.creator = creator
				new_obj.save()
			return {'code': 0, 'msg': 'success'}
		except Exception as e:
			return {'code': 1, 'msg': str(e)}	

def exdb(fields, model, filters, sheet_name='sheet1', istemp=False, query_type='orgin', connector={}, **kwargs):
	"""
		导出主函数
		connector: 数据库连接参数
		sql: 查询sql
		sheet_name: excel的sheet页名称
		fields: 字段名及excel里的title名映射关系
			fields: {
				'field': 'field_name',
			}
		query_type: 查询数据库的方式，支持 [原生sql:'orgin', orm: 'orm']
		kwargs:
			sql: 'select * from t_0_xxx where delflag=false '	
			orm: {
					'sys': True,
					'model': 'System'
				} # 系统表
				或：
				{
					'sys': False,
					'model': 'client'
				} # 业务表
		
	"""
	if query_type == 'orgin':
		exobj = ZBusiness(connector)
		sql_exp = kwargs.get('sql')
		res = exobj.exdb(fields, sheet_name=sheet_name, istemp=istemp, sql=sql_exp, query_type=query_type, **kwargs)
	elif query_type == 'orm':
		exobj = ZBusiness(connector)
		res = exobj.exdb(fields, model=model, filters=filters, istemp=istemp, sheet_name=sheet_name, query_type=query_type, **kwargs)
	else:
		raise  OdpException('不支持当前query_type格式')
	return res


def imdb(connector, file_path, model='', titleline=1, startline = 2, fieldmaps=None, comp=0, **kwargs):
	"""导入主函数"""
	imobj = ZBusiness(connector)
	creator = kwargs.get('creator')
	imobj.imdb(file_path, model, titleline, startline, fieldmaps, creator, comp, **kwargs)
	return {'code': 0, 'msg': 'success'}
	

if __name__ == '__main__':
	connector = {
				'user': 'odp',
				'password': '12345678',
				'ip': '127.0.0.1',
				'port': '5432',
				'db': 'odp'
			}
	sheet_name = '省库检测记录'
	fields = {
#			'numb': '序号',
			'the_date': '日期',
			'product_name': '产品名称',
			'specifications': '规格',
			'supplier': '供应商',
			'optical_cable_number': '光缆盘号',
			'order_number': '订单号',
			'check_date': '检测时间',
			'check_area': '检测地点',
			'check_result': '检测结果',
			'unqualified_description': '不合格说明',
		}
	sql = "select row_number() over (order by t_0_province_checked_record.id asc) as numb, left(cast(max(the_date) as VARCHAR),10), product_name, specifications, supplier, optical_cable_number, order_number, left(cast(max(check_date) as VARCHAR),10), check_area, (case when check_result=1 then '检测中' when check_result =2 then '合格' when check_result =3 then '不合格' when check_result =4 then '合格但有缺陷' end), unqualified_description from t_0_province_checked_record where delflag=false group by product_name, specifications, supplier, optical_cable_number,check_area, check_result, t_0_province_checked_record.order_number, t_0_province_checked_record.unqualified_description, t_0_province_checked_record.id"
	filters = ' delflag=false '
	model = 'province_checked_record'
	res = exdb(fields, model=model, sheet_name=sheet_name, filters=filters, sql=sql, connector=connector, query_type='orm')
	print(res)
	fieldmaps = {
        '日期': 'the_date',
        '产品名称': 'product_name',
        '规格': 'specifications',
        '供应商': 'supplier',
        '入库单号': 'order_number',
        '检测时间': 'check_date',
        '检测地点': 'check_area',
        '检测结果': 'check_result',
        '不合格说明': 'unqualified_description'
    	}
	additional = {
        '检测中': 1,
        '合格': 2,
        '不合格': 3,
        '合格但有缺陷':4,
    }
	typedict = {
		0: 'int',
		1: 'date',
		2: 'string',
		3: 'string',
		4: 'string',
		5: 'string',
		6: 'date',
		7: 'string',
		8: 'additional',
		9: 'string',	
	}
	#res = imdb(connector, '省库验货导入模板.xlsx', model='province_checked_record', titleline=1, startline=2, fieldmaps=fieldmaps, typedict=typedict, comp=0)

