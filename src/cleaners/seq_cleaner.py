class sequential_cleaner:
    def __init__(self, wanted_dict):
        self.wanted_run = wanted_dict.get('run_confs', None)
        self.wanted_job = wanted_dict.get('job_id', None)
        self.wanted_job_meta = wanted_dict.get('job_meta_info', None)
        self.wanted_job_level = wanted_dict.get('job_level_metrics', None) 
        self.wanted_vertex = wanted_dict.get('vertex_id', None)
        self.wanted_vertex_level = wanted_dict.get('vertex_level_metrics', None)
        self.wanted_sb_level = wanted_dict.get('sub_level_metrics', None)

        self.head = self.make_head()
        self.dataset = []
    
    def make_head(self,):
        res = []
        if self.wanted_run:
            res += self.wanted_run
        if self.wanted_job:
            res += ['job_id']
        if self.wanted_job_meta:
            res += self.wanted_job_meta
        if self.wanted_job_level:
            tmp = []
            for e in self.wanted_job_level:
                if e != 'pseudoSrcInputRate':
                    tmp.append(e)
                    continue
                tmp += ['pseudoSrcNumBytesInPerSecond', 'pseudoSrcNumRecordsInPerSecond']
            res += tmp
        if self.wanted_vertex:
            if self.wanted_vertex == 'idx':
                res += ['vertex_idx']
            else:
                res += ['vertex_id']
        if self.wanted_vertex_level:
            res += self.wanted_vertex_level
        if self.wanted_sb_level:
            res += ['sub_idx']
            res += self.wanted_sb_level
        return res
    
    def get_head(self,):
        return self.head
    
    def help_select_wanted(self, attr_dict, want_list, sub_key = 'value', m_num = None):
        feasible_target = len(want_list) if not m_num else m_num
        res = []
        feasible_counter = 0
        for k in want_list:
            if k in attr_dict.keys():
                feasible_counter += 1
                if sub_key:
                    res.append(attr_dict[k][sub_key])
                else:
                    res.append(attr_dict[k])
            else:
                break
        if feasible_counter < feasible_target:
            # means not all wanted is here, so skip
            return []
        return res
    
    def make_run_attributes(self, run_confs):
        return self.help_select_wanted(run_confs, self.wanted_run, '')
    
    def make_job_meta_attributes(self, job_metrics):
        return self.help_select_wanted(job_metrics['job_meta_info'], self.wanted_job_meta, '')
    
    def make_job_level_pseudoSrcInputRate(self, job_metrics):
        # observe the 'sum' of first vertex's ‘numBytesOutPerSecond’ and 'numRecordsInPerSecond' per second
        first_vertex = job_metrics['vertice_metrics'][0]
        vid = first_vertex['vertex_id']
        # for debug
        assert 'Source' in job_metrics['job_meta_info']['vertices'][0]['name'] and vid == job_metrics['job_meta_info']['vertices'][0]['id'], 'err..fuck'

        return [first_vertex['vertex_level_metrics']['numBytesOutPerSecond']['sum'],
               first_vertex['vertex_level_metrics']['numRecordsOutPerSecond']['sum'],]
    
    def make_job_level_jobIsBackPressured(self, job_metrics):
        # observe all subtasks to see any is back-pressured
        isBackPressured = False
        for v in job_metrics['vertice_metrics']:
            for sb in v['sub_level_metrics']:
                if sb['isBackPressured']['value'] == 'true':
                    isBackPressured = True
                    break
            if isBackPressured:
                break
        return str(isBackPressured)
    
    def make_job_level_jobVerParallelisms(self, job_metrics):
        res = []
        for v in job_metrics['job_meta_info']['vertices']:
            res.append(v['parallelism'])
        return str(res)
    
    def make_job_level_attributes(self, job_metrics):
        # TODO: needs further config info
        # no feasible protection for job-level metrics collection, be careful
        res = []
        # 2 special attributes: (1) pseudoSrcInputRate, (2) jobIsBackPressured
        for k in self.wanted_job_level:
            if k in job_metrics['job_level_metrics'].keys():
                res.append(max([e['value'] for e in v.values()]))
            # special job-level metrics
            elif  k == 'pseudoSrcInputRate':
                res += self.make_job_level_pseudoSrcInputRate(job_metrics) # returns 2 values
            elif k == 'jobIsBackPressured':
                res.append(self.make_job_level_jobIsBackPressured(job_metrics))
            elif k == 'jobVerParallelisms':
                res.append(self.make_job_level_jobVerParallelisms(job_metrics))

        return res

    
    def make_ver_level_attributes(self, job_metrics):
        dicts1 = job_metrics['job_meta_info']['vertices']
        dicts2 = job_metrics['vertice_metrics']
        wanted_list1 = [e for e in self.wanted_vertex_level if e in dicts1[0].keys()]
        wanted_list2 = [e for e in self.wanted_vertex_level if e not in wanted_list1]
        v_num = len(dicts1)
        res1 = [self.help_select_wanted(dicts1[i], wanted_list1, '') for i in range(v_num)]
        res2 = [self.help_select_wanted(dicts2[i]['vertex_level_metrics'], wanted_list2, 'avg') for i in range(v_num)]
        res = []
        for i in range(v_num):
            res.append(res1[i] + res2[i])
        return res
    
    def make_sb_level_attributes(self, vertice_metrics):
        res = []
        for vm in vertice_metrics:
            v_res = []
            for sb_idx in range(len(vm['sub_level_metrics'])):
                v_res.append([sb_idx] + self.help_select_wanted(vm['sub_level_metrics'][sb_idx], self.wanted_sb_level))
            res.append(v_res)
        return res # 3-d array
    
    def assemble_rows(self, sub_rows_1=[], sub_rows_2=[], sub_rows_3=[]):
        res = [] # collected samples
        
        sub_rows_1_combined = []
        for r in sub_rows_1:
            sub_rows_1_combined += r
        
        sub_rows_2_combined = []
        for i in range(len(sub_rows_2[0] if sub_rows_2 else [])): # v num
            tmp = []
            for r in sub_rows_2:
                tmp += r[i]
            sub_rows_2_combined.append(tmp)
        
        # sub_rows_3_combined do not need to re-assemble
        # assumes there must be sub-task level metrics: (or, sub_rows_3 is not empty)
        if sub_rows_3:
            for i in range(len(sub_rows_3)): # i->vertex
                for j in range(len(sub_rows_3[i])): # j->subtask
                    a_sample = []
                    a_sample += sub_rows_1_combined
                    if sub_rows_2_combined:
                        sbr = sub_rows_2_combined[i]
                        if not sbr: continue
                        a_sample += sbr
                    sbr = sub_rows_3[i][j]
                    if not sbr: continue
                    a_sample += sbr
                    res.append(a_sample)
        elif sub_rows_2:
            # in the case no subtask-level records
            for i in range(len(sub_rows_2_combined)):
                a_sample = []
                a_sample += sub_rows_1_combined
                vtr = sub_rows_2_combined[i]
                if not vtr: continue
                a_sample += vtr
                res.append(a_sample)
        else:
            # if only need job-level records
            res = [sub_rows_1_combined]

        return res # return a set of tabular samples
    
    def clean_row(self, row):
        run_confs = eval(row['run_confs']) # dict
        job_metrics = eval(row['job_metrics']) # dict
        
        cleaned_rows = []
        
        sub_rows_1 = [] # 1d
        sub_rows_2 = [] # 2d
        sub_rows_3 = [] # 3d
        
        if self.wanted_run:
            run_attrs = self.make_run_attributes(run_confs) # 1-list
            sub_rows_1.append(run_attrs)
        if self.wanted_job:
            job_attrs = [job_metrics['job_id']]
            sub_rows_1.append(job_attrs)
        if self.wanted_job_meta:
            job_meta_attrs = self.make_job_meta_attributes(job_metrics)
            sub_rows_1.append(job_meta_attrs)
        if self.wanted_job_level:
            job_level_attrs = self.make_job_level_attributes(job_metrics)
            sub_rows_1.append(job_level_attrs)
        
        if self.wanted_vertex:
            if self.wanted_vertex == 'idx':
                ver_attrs = [[i] for i in range(len(job_metrics['vertice_metrics']))]
            else:
                ver_attrs = [[e['vertex_id']] for e in job_metrics['vertice_metrics']]
            sub_rows_2.append(ver_attrs)
        if self.wanted_vertex_level:
            ver_level_attrs = self.make_ver_level_attributes(job_metrics)
            sub_rows_2.append(ver_level_attrs)

        if self.wanted_sb_level:
            sb_level_attrs = self.make_sb_level_attributes(job_metrics['vertice_metrics'])
            sub_rows_3.append(sb_level_attrs)

        res = self.assemble_rows(sub_rows_1, sub_rows_2, sub_rows_3[0] if len(sub_rows_3)>=1 else [])

        self.dataset += res
        return row