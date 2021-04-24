import aiohttp
import asyncio

'''
EXAMPLE:

    # single task request:
        import asyncio
        reps = asyncio.run(*[async_curl.request(url)])
        print(reps)


    # Sample request:
        task = [url1,url2,...]
        curl = async_curl(task)
        for reps in curl.run_task(10):
            print(reps)

    # Custom configuration request:
        The second argument refers to the function `asyncio.request()`
        task = [(url, {
                    'method': 'GET',
                    'params': params,
                    'headers': {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36'},
                }),
                (url2,params2),...]
        curl = async_curl(task)
        for reps in curl.run_task(10):
            print(reps)
'''


class async_curl():
    '''
    异步curl请求
    '''

    def __init__(self, task_url_list=[]) -> None:
        '''
        [summary]

        Args:
            task_url_list (list, optional): [待处理的任务列表]. Defaults to [].
        '''
        super().__init__()
        self.task_done = []  # 已完成的任务列表
        self.set_task(task_url_list)

    def set_task(self, task_url_list):
        self.task_url_list = task_url_list
        self._emit = True

    def append_task(self, task):
        self._check_task(task)
        self.task_url_list.append(task)
        self._emit = True

    def _check_task(self, task):
        assert isinstance(task, (str, tuple)
                          ), f'task type error :{type(task)} . only support: str、tuple '

    # get 请求

    async def request(self, request_task, method='GET'):
        '''
        curl请求

        Args:
            request_task ([str|tuple]): [请求任务]
            method (str, GET,POST): [请求默认方法 request_task参数中可指定]. Defaults to 'GET'.
        '''

        kw = {}
        self._check_task(request_task)
        if isinstance(request_task, str):
            request_task = (request_task, {'method': method})

        assert isinstance(request_task, tuple), '$request_task format error.'
        url, kw = request_task
        # 处理kwargs传入的参数
        if 'method' in kw:
            method = kw['method']
            del kw['method']
        if 'url' in kw:
            del kw['url']

        async with aiohttp.request(method, url, **kw) as response:
            # print(self.task_url_list[index])
            # response.request_info # 请求信息
            return await response.text()

    async def _bulk_task(self, num, current_start_index):
        '''
        批量创建异步任务

        Args:
            num ([type]): [单次请求的并发数]
            current_start_index (int, optional): [description].  

        Returns:
            [type]: [description]
        '''
        task = []
        for i in range(num):  # 每次num个连接并发进行请求
            task.append(asyncio.create_task(self.request(
                self.task_url_list[current_start_index])))
            current_start_index += 1
        if task:
            return await asyncio.gather(*task)

    # 主要进行chunk操作的函数
    def run_task(self, chunk_num):
        """运行分块处理的批量任务

        Arguments:
            chunk_num int 每次并发请求数

        Yields:
            返回收集的异步任务运行结果
        """
        while self.task_url_list:
            total = len(self.task_url_list)
            start_index = 0  # 当前分块开始的页数 因为下面会task_url_list.pop(0).这里都是从0开始读取
            haldle_num = min(chunk_num, total)  # 当前需要并发处理的数量

            # print('当前分块需要处理的总数：{},当前分块开始页数:{}'.format(haldle_num,start_index))
            rel = asyncio.run(self._bulk_task(haldle_num, start_index))

            # 转移到已完成队列
            [self.task_done.append(self.task_url_list.pop(0))
             for _ in range(haldle_num) if haldle_num]
            yield (rel)
