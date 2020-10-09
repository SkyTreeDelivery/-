class PanoCur:
    def __init__(self,pid):
        self.pid=pid
        self.rid=None
        self.x=None
        self.y=None
        self.time=None
        self.path=None
        self.heading=None
        self.pitch=None
        self.type=None
        self.timeGroupId=None
        self.isnode=False # 默认不是节点
        self.order=None
        self.isendpoint=False # 默认不是端点