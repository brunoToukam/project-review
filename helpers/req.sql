select oa.* from octavt_oeuv oo
    left join octavtj_oeuv_motifattente om on oo.idoeuv = om.idoeuv
    inner join octav1_app.octavt_cess oc on oo.idoeuv = oc.idoeuv
    inner join octav1_app.octavt_eltter oe on oc.idtercmplx = oe.idtercmplx
    inner join octav1_app.temp_paf_terr tpt on oe.cdepaysiso4n = tpt.cdepaysiso4n
    inner join octav1_app.octavt_jeucle oj on oc.idcess = oj.idcess
    inner join octav1_app.octavt_ayantdrt oa on oj.idjeucle = oa.idjeucle
         where oo.datdbtsys < '2024-04-17'
         and (oo.datfinsys >  '2024-04-17' or oo.datfinsys is null)
         and oo.ide12 = '9411414311'
         and oj.cdetypdrtsacem = 'DE';
         -- TODO prendre la prio la plus faible dans octavt_cess