truncate table stage.stg_exam_results_sirio;

copy stage.stg_exam_results_sirio
from '/mnt/sharedstorage/hsl_lab_result_1.csv'
delimiters '|' csv header encoding 'utf-8';

truncate table stage.stg_exam_results_fleury;

copy stage.stg_exam_results_fleury
from '/mnt/sharedstorage/Grupo_Fleury_Dataset_Covid19_Resultados_Exames.csv'
delimiters '|' csv header encoding 'latin1';

truncate table stage.stg_exam_results_einstein;

copy stage.stg_exam_results_einstein
from '/mnt/sharedstorage/einstein_full_dataset_exames.csv'
delimiters '|' csv header encoding 'utf-8';

-- pacientes
truncate table stage.stg_patients_sirio;

copy stage.stg_patients_sirio
from '/mnt/sharedstorage/hsl_patient_1.csv'
delimiters '|' csv header encoding 'utf-8';

truncate table stage.stg_patients_fleury;

copy stage.stg_patients_fleury
from '/mnt/sharedstorage/Grupo_Fleury_Dataset_Covid19_Pacientes.csv'
delimiters '|' csv header encoding 'latin1';

truncate table stage.stg_patients_einstein;

copy stage.stg_patients_einstein
from '/mnt/sharedstorage/einstein_full_dataset_paciente.csv'
delimiters '|' csv header encoding 'utf-8';