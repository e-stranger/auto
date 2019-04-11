from mcauto.core.process.process import DatabaseSumQATask
PROC_NAME = 'CheckDCMPivotSumQA'

def test_ctc_sum(test_adidas_sqlalchemy):
    DatabaseSumQATask(engine=test_adidas_sqlalchemy, proc=PROC_NAME, params=['CTC'])

def test_vtc_sum(test_adidas_sqlalchemy):
    DatabaseSumQATask(engine=test_adidas_sqlalchemy, proc=PROC_NAME, params=['VTC'])

def test_ctr_sum(test_adidas_sqlalchemy):
    DatabaseSumQATask(engine=test_adidas_sqlalchemy, proc=PROC_NAME, params=['CTR'])

def test_vtr_sum(test_adidas_sqlalchemy):
    DatabaseSumQATask(engine=test_adidas_sqlalchemy, proc=PROC_NAME, params=['VTR'])