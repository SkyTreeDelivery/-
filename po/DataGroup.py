class DataGroup:
    def __init__(self):
        self.panos = [] # [PanoCur]
        self.panosHistory = [] # [PanoHistory]
        self.panoCurIndex = None
        self.locType = None # PanoType.START / END / MIDDLE
        self.roadFragment = None # RoadFragment
        self.topo = []  # [RoadFragmentTopo]
        self.hisGroup = None # GroupHistory