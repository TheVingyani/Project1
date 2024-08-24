'''
Create (ie. INSERT) methods for TCRD.DBadaptor 

Steve Mathias
smathias@salud.unm.edu
Time-stamp: <2022-04-01 12:19:09 smathias>
Time-stamp: <2021-09-28 12:10:00 smathias>
'''
from mysql.connector import Error
from contextlib import closing

class CreateMethodsMixin:
  
  def ins_dataset(self, init):
    if 'name' in init and 'source' in init :
      params = [init['name'], init['source']]
    else:
      self.warning("Invalid parameters sent to ins_dataset(): ", init)
      return False
    cols = ['name', 'source']
    vals = ['%s','%s']
    for optcol in ['app', 'app_version', 'datetime', 'url', 'comments']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO dataset (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        dataset_id = curs.lastrowid
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_dataset(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return dataset_id

  def ins_provenance(self, init):
    if 'dataset_id' in init and 'table_name' in init :
      params = [init['dataset_id'], init['table_name']]
    else:
      self.warning("Invalid parameters sent to ins_provenance(): ", init)
      return False
    cols = ['dataset_id', 'table_name']
    vals = ['%s','%s']
    for optcol in ['column_name', 'where_clause', 'comment']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO provenance (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_target(self, init):
    '''
    Function  : Insert a target and all associated data provided.
    Arguments : Dictionary containing target data.
    Returns   : Integer containing target.id
    Example   : tid = dba->ins_target(init) ;
    Scope     : Public
    Comments  : This only handles data parsed from UniProt XML entries in load-UniProt.py
    '''
    if 'name' in init and 'ttype' in init:
      params = [init['name'], init['ttype']]
    else:
      self.warning(f"Invalid parameters sent to ins_target(): {init}")
      return False
    cols = ['name', 'ttype']
    vals = ['%s','%s']
    for optcol in ['description', 'comment']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO target (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    target_id = None
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        target_id = curs.lastrowid
      except Error as e:
        self._logger.error(f"MySQL Error in ins_target(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    for protein in init['components']['protein']:
      protein_id = self.ins_protein(protein, commit=True)
      if not protein_id:
        return False
      sql = "INSERT INTO t2tc (target_id, protein_id) VALUES (%s, %s)"
      params = (target_id, protein_id)
      self._logger.debug(f"SQLpat: {sql}")
      self._logger.debug(f"SQLparams: {params}")
      with closing(self._conn.cursor()) as curs:
        try:
          curs.execute(sql, tuple(params))
        except Error as e:
          self._logger.error(f"MySQL Error in ins_target(): {e}")
          self._logger.error(f"SQLpat: {sql}")
          self._logger.error(f"SQLparams: {params}")
          self._conn.rollback()
          return False
      try:
        self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error(f"MySQL commit error in ins_target(): {e}")
        return False
    return target_id

  def ins_protein(self, init, commit=True):
    '''
    Function  : Insert a protein and all associated data provided.
    Arguments : Dictionary containing target data.
    Returns   : Integer containing target.id
    Example   : pid = dba->ins_protein(init) ;
    Scope     : Public
    Comments  : This only handles data parsed from UniProt XML entries in load-UniProt.py
    '''
    if 'name' in init and 'description' in init and 'uniprot' in init:
      params = [init['name'], init['description'], init['uniprot']]
    else:
      self.warning(f"Invalid parameters sent to ins_protein(): {init}")
      return False
    cols = ['name', 'description', 'uniprot']
    vals = ['%s','%s', '%s']
    for optcol in ['up_version', 'geneid', 'sym', 'family', 'chr', 'seq']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO protein (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    protein_id = None
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        protein_id = curs.lastrowid
      except Error as e:
        self._logger.error(f"MySQL Error in ins_protein(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if 'aliases' in init:
      for d in init['aliases']:
        d['protein_id'] = protein_id
        rv = self.ins_alias(d, commit=True)
        if not rv:
          return False
    if 'xrefs' in init:
      for d in init['xrefs']:
        d['protein_id'] = protein_id
        rv = self.ins_xref(d, commit=True)
        if not rv:
          return False
    if 'tdl_infos' in init:
      for d in init['tdl_infos']:
        d['protein_id'] = protein_id
        rv = self.ins_tdl_info(d, commit=True)
        if not rv:
          return False
    if 'goas' in init:
      for d in init['goas']:
        d['protein_id'] = protein_id
        rv = self.ins_goa(d, commit=True)
        if not rv:
          return False
    if 'expressions' in init:
      for d in init['expressions']:
        d['protein_id'] = protein_id
        rv = self.ins_expression(d, commit=True)
        if not rv:
          return False
    if 'pathways' in init:
      for d in init['pathways']:
        d['protein_id'] = protein_id
        rv = self.ins_pathway(d, commit=True)
        if not rv:
          return False
    if 'diseases' in init:
      for d in init['diseases']:
        d['protein_id'] = protein_id
        rv = self.ins_disease(d, commit=True)
        if not rv:
          return False
    if 'features' in init:
      for d in init['features']:
        d['protein_id'] = protein_id
        rv = self.ins_feature(d, commit=True)
        if not rv:
          return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error(f"MySQL commit error in ins_protein(): {e}")
        return False
    return protein_id

  def ins_nhprotein(self, init):
    if 'uniprot' in init and 'name' in init and 'species' in init and 'taxid' in init:
      params = [init['uniprot'], init['name'], init['species'], init['taxid']]
    else:
      self.warning(f"Invalid parameters sent to ins_nhprotein(): {init}")
      return False
    cols = ['uniprot', 'name', 'species', 'taxid']
    vals = ['%s','%s','%s','%s']
    for optcol in ['sym', 'description', 'geneid', 'stringid']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO nhprotein (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    nhprotein_id = None
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        nhprotein_id = curs.lastrowid
      except Error as e:
        self._logger.error(f"MySQL Error in ins_nhprotein(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if 'xrefs' in init:
      for d in init['xrefs']:
        d['nhprotein_id'] = nhprotein_id
        rv = self.ins_xref(d, commit=False)
        if not rv:
          return False
    try:
      self._conn.commit()
    except Error as e:
      self._conn.rollback()
      self._logger.error(f"MySQL commit error in ins_nhprotein(): {e}")
      return False
    return nhprotein_id

  def ins_alias(self, init, commit=True):
    if 'protein_id' not in init or 'type' not in init or 'dataset_id' not in init or 'value' not in init:
      self.warning("Invalid parameters sent to ins_alias(): ", init)
      return False
    sql = "INSERT INTO alias (protein_id, type, dataset_id, value) VALUES (%s, %s, %s, %s)"
    params = (init['protein_id'], init['type'], init['dataset_id'], init['value'])
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        self._logger.error(f"MySQL Error in ins_alias(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_alias(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_xref(self, init, commit=True):
    if 'xtype' in init and 'dataset_id' in init and 'value' in init:
      params = [init['xtype'], init['dataset_id'], init['value']]
    else:
      self.warning(f"Invalid parameters sent to ins_xref(): {init}")
      return False
    if 'protein_id' in init:
      cols = ['protein_id', 'xtype', 'dataset_id', 'value']
      vals = ['%s','%s','%s','%s']
      params.insert(0, init['protein_id'])
    elif 'target_id' in init:
      cols = ['target_id', 'xtype', 'dataset_id', 'value']
      vals = ['%s','%s','%s','%s']
      params.insert(0, init['target_id'])
    elif 'nhprotein_id' in init:
      cols = ['nhprotein_id', 'xtype', 'dataset_id', 'value']
      vals = ['%s','%s','%s','%s']
      params.insert(0, init['nhprotein_id'])
    else:
      self.warning("Invalid parameters sent to ins_xref(): ", init)
      return False
    if 'xtra' in init:
      cols.append('xtra')
      vals.append('%s')
      params.append(init['xtra'])
    sql = "INSERT INTO xref (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    protein_id = None
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        pass
        # if 'Duplicate entry' in e[1] and "key 'xref_idx5'" in e[1]:
        #   pass
        # else:
        #   self._logger.error(f"MySQL Error in ins_xref(): {e}")
        #   self._logger.error(f"SQLpat: {sql}")
        #   self._logger.error(f"SQLparams: {params}")
        #   self._conn.rollback()
        #   return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_xref(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_tdl_info(self, init, commit=True):
    if 'itype' in init:
      itype = init['itype']
    else:
      self.warning(f"Invalid parameters sent to ins_tdl_info(): {init}")
      return False
    if 'string_value' in init:
      val_col = 'string_value'
      value = init['string_value']
    elif 'integer_value' in init:
      val_col = 'integer_value'
      value = init['integer_value']
    elif 'number_value' in init:
      val_col = 'number_value'
      value = init['number_value']
    elif 'boolean_value' in init:
      val_col = 'boolean_value'
      value = init['boolean_value']
    elif 'date_value' in init:
      val_col = 'date_value'
      value = init['date_value']
    else:
      self.warning(f"Invalid parameters sent to ins_tdl_info(): {init}")
      return False
    if 'protein_id' in init:
      xid = init['protein_id']
      sql = "INSERT INTO tdl_info (protein_id, itype, %s)" % val_col
    elif 'target_id' in init:
      xid = init['target_id']
      sql = "INSERT INTO tdl_info (target_id, itype, %s)" % val_col
    else:
      self.warning(f"Invalid parameters sent to ins_tdl_info(): {init}")
      return False
    sql += " VALUES (%s, %s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {xid}, {itype}, {value}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, (xid, itype, value))
      except Error as  e:
        self._logger.error(f"MySQL Error in ins_tdl_info(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {xid}, {itype}, {value}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_tdl_info(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_goa(self, init, commit=True):
    if 'protein_id' in init and 'go_id' in init:
      params = [init['protein_id'], init['go_id']]
    else:
      self.warning(f"Invalid parameters sent to ins_goa(): {init}")
      return False
    cols = ['protein_id', 'go_id']
    vals = ['%s','%s']
    for optcol in ['go_term', 'evidence', 'goeco', 'assigned_by']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO goa (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
         self._logger.error(f"MySQL Error in ins_goa(): {e}")
         self._logger.error(f"SQLpat: {sql}")
         self._logger.error(f"SQLparams: {params}")
         self._conn.rollback()
         return False
      if commit:
        try:
          self._conn.commit()
        except Error as e:
          self._logger.error(f"MySQL commit error in ins_goa(): {e}")
          self._conn.rollback()
          return False
    return True
  
  def ins_vitamin(self, init, commit=True):
    
    cols = []
    vals = []
    params=[]
    for optcol in ['Vitamin_name', 'Vitamin_id','geneid', 'protein_id', 'Uniprot', 'Reviewed', 'Entry_Name', 'Protein_names', 'Gene_Names', 'Organism','Length']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO vitamin (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
         self._logger.error(f"MySQL Error in ins_vitamin(): {e}")
         self._logger.error(f"SQLpat: {sql}")
         self._logger.error(f"SQLparams: {params}")
         self._conn.rollback()
         return False
      if commit:
        try:
          self._conn.commit()
        except Error as e:
          self._logger.error(f"MySQL commit error in ins_goa(): {e}")
          self._conn.rollback()
          return False
    return True

  def ins_pathway(self, init, commit=True):
    if 'pwtype' in init and 'name' in init:
      pwtype = init['pwtype']
      name = init['name']
    else:
      self.warning(f"Invalid parameters sent to ins_pathway(): {init}")
      return False
    if 'protein_id' in init:
      cols = ['protein_id', 'pwtype', 'name']
      vals = ['%s','%s', '%s']
      params = [ init['protein_id'], pwtype, name ]
    elif 'target_id' in init:
      cols = ['target_id', 'pwtype', 'name']
      vals = ['%s','%s','%s']
      params = [ init['target_id'], pwtype, name ]
    else:
      self.warning(f"Invalid parameters sent to ins_pathway(): {init}")
      return False
    for optcol in ['id_in_source', 'description', 'url']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO pathway (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
         self._logger.error(f"MySQL Error in ins_pathway(): {e}")
         self._logger.error(f"SQLpat: {sql}")
         self._logger.error(f"SQLparams: {params}")
         self._conn.rollback()
         return False
      if commit:
        try:
          self._conn.commit()
        except Error as e:
          self._logger.error(f"MySQL commit error in ins_pathway(): {e}")
          self._conn.rollback()
          return False
    return True

  def ins_disease(self, init, commit=True):
    if 'dtype' in init and 'name' in init:
      cols = ['dtype', 'name']
      # Replace the disease type uniprot to 'UniProt Disease' in std form.
      dtype = init['dtype']
      if not dtype:
          dtype = ''
      if (dtype.lower() == 'uniprot'):
          dtype = 'UniProt Disease'
      params = [dtype, init['name']]
    else:
      self.warning("Invalid parameters sent to ins_disease(): ", init)
      return False
    if 'protein_id' in init:
      cols.insert(0, 'protein_id')
      vals = ['%s','%s','%s']
      params.insert(0, init['protein_id'])
    elif 'nhprotein_id' in init:
      cols.insert(0, 'nhprotein_id')
      vals = ['%s','%s','%s']
      params.insert(0, init['nhprotein_id'])
    else:
      self.warning(f"Invalid parameters sent to ins_disease(): {init}")
      return False
    for optcol in ['did', 'evidence', 'zscore', 'conf', 'description', 'reference', 'drug_name', 'log2foldchange', 'pvalue', 'score', 'source', 'O2s', 'S2O']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO disease (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
      except Error as e:
        
        self._logger.error(f"MySQL Error in ins_disease(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        
        self._logger.error(f"MySQL commit error in ins_disease(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_phenotype(self, init, commit=True):
    if 'ptype' not in init:
      self.warning("Invalid parameters sent to ins_phenotype(): ", init)
      return False
    if 'protein_id' in init:
      cols = ['protein_id', 'ptype']
      vals = ['%s','%s']
      params = [init['protein_id'], init['ptype']]
    elif 'nhprotein_id' in init:
      cols = ['nhprotein_id', 'ptype']
      vals = ['%s','%s']
      params = [init['nhprotein_id'], init['ptype']]
    else:
      self.warning(f"Invalid parameters sent to ins_phenotype(): {init}")
      return False
    for optcol in ['trait', 'top_level_term_id', 'top_level_term_name', 'term_id', 'term_name', 'term_description', 'p_value', 'percentage_change', 'effect_size', 'procedure_name', 'parameter_name', 'gp_assoc', 'statistical_method', 'sex']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO phenotype (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
      except Error as e:
        self._logger.error(f"MySQL Error in ins_phenotype(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_phenotype(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_expression(self, init, commit=True):
    if 'etype' in init and 'tissue' in init:
      params = [init['etype'], init['tissue']]
    else:
      self.warning(f"Invalid parameters sent to ins_expression(): {init}")
      return False
    cols = ['etype', 'tissue']
    vals = ['%s','%s']
    if 'protein_id' in init:
      cols = ['protein_id', 'etype', 'tissue']
      vals = ['%s','%s','%s']
      params.insert(0, init['protein_id'])
    elif 'target_id' in init:
      cols = ['target_id', 'etype', 'tissue']
      vals = ['%s','%s','%s']
      params.insert(0, init['target_id'])
    else:
      self.warning(f"Invalid parameters sent to ins_expression(): {init}")
      return False
    for optcol in ['qual_value', 'string_value', 'number_value', 'boolean_value', 'pubmed_id', 'evidence', 'zscore', 'conf', 'oid', 'confidence', 'url', 'cell_id', 'uberon_id']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO expression (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    # print(sql)
    # print(params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
      except Error as e:
        self._logger.error(f"MySQL Error in ins_expression(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_expression(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_feature(self, init, commit=True):
    if 'protein_id' in init and 'type' in init:
      params = [init['protein_id'], init['type']]
    else:
      self.warning(f"Invalid parameters sent to ins_feature(): {init}")
      return False
    cols = ['protein_id', 'type']
    vals = ['%s','%s']
    for optcol in ['description', 'srcid', 'evidence', 'position', 'begin', 'end']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO feature (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        self._logger.error(f"MySQL Error in ins_feature(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_feature(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_drgc_resource(self, init, commit=True):
    if 'target_id' in init and 'resource_type' in init and 'json' in init:
      cols = ['target_id', 'resource_type', 'json']
      vals = ['%s','%s','%s']
      params = [init['target_id'], init['resource_type'], init['json']]
    else:
      self.warning("Invalid parameters sent to ins_drgc_resource(): ", init)
      return False
    sql = "INSERT INTO drgc_resource (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%(", ".join([str(p) for p in params])))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_ortholog(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL commit error in ins_drgc_resource(): %s"%str(e))
        return False
    
    return True

  def ins_pmscore(self, init, commit=True):
    if 'protein_id' in init and 'year' in init and 'score' in init:
      params = [init['protein_id'], init['year'], init['score']]
    else:
      self.warning(f"Invalid parameters sent to ins_pmscore(): {init}")
      return False
    sql = "INSERT INTO pmscore (protein_id, year, score) VALUES (%s, %s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
      except Error as e:
        self._logger.error(f"MySQL Error in ins_pmscore(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL commit error in ins_pmscore(): {e}")
        self._conn.rollback()
        return False
    return True

  def ins_extlink(self, init):
    if 'protein_id' not in init or 'source' not in init or 'url' not in init:
      self.warning("Invalid parameters sent to ins_extlink(): ", init)
      return False
    sql = "INSERT INTO extlink (protein_id, source, url) VALUES (%s, %s, %s)"
    params = (init['protein_id'], init['source'], init['url'])
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_extlink(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_mondo(self, init):
    if 'mondoid' in init and 'name' in init:
      cols = ['mondoid', 'name']
      vals = ['%s','%s']
      params = [init['mondoid'], init['name']]
    else:
      self.warning("Invalid parameters sent to ins_mondo(): ", init)
      return False
    for optcol in ['def', 'comment']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO mondo (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        self._logger.error(f"MySQL Error in ins_mondo(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if 'parents' in init:
      for parentid in init['parents']:
        sql = "INSERT INTO mondo_parent (mondoid, parentid) VALUES (%s, %s)"
        params = [init['mondoid'], parentid]
        self._logger.debug(f"SQLpat: {sql}")
        self._logger.debug(f"SQLparams: {params}")
        with closing(self._conn.cursor()) as curs:
          try:
            curs.execute(sql, params)
          except Error as e:
            self._logger.error(f"MySQL Error in ins_mondo(): {e}")
            self._logger.error(f"SQLpat: {sql}")
            self._logger.error(f"SQLparams: {params}")
            self._conn.rollback()
            return False
    if 'xrefs' in init:
      for xref in init['xrefs']:
        if 'source' in xref:
          sql = "INSERT INTO mondo_xref (mondoid, db, value, equiv_to, source_info) VALUES (%s, %s, %s, %s, %s)"
          params = [init['mondoid'], xref['db'], xref['value'], xref['equiv_to'], xref['source']]
        else:
          sql = "INSERT INTO mondo_xref (mondoid, db, value, equiv_to) VALUES (%s, %s, %s, %s)"
          params = [init['mondoid'], xref['db'], xref['value'], xref['equiv_to']]
        self._logger.debug(f"SQLpat: {sql}")
        self._logger.debug(f"SQLparams: {params}")
        with closing(self._conn.cursor()) as curs:
          try:
            curs.execute(sql, params)
          except Error as e:
            self._logger.error(f"MySQL Error in ins_mondo(): {e}")
            self._logger.error(f"SQLpat: {sql}")
            self._logger.error(f"SQLparams: {params}")
            self._conn.rollback()
            return False
    try:
      self._conn.commit()
    except Error as e:
      self._logger.error(f"MySQL commit error in ins_mondo(): {e}")
      self._conn.rollback()
      return False
    return True
  
  def ins_uberon(self, init):
    if 'uid' in init and 'name' in init:
      cols = ['uid', 'name']
      vals = ['%s','%s']
      params = [init['uid'], init['name']]
    else:
      self.warning("Invalid parameters sent to ins_uberon(): ", init)
      return False
    for optcol in ['def', 'comment']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO uberon (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        self._logger.error(f"MySQL Error in ins_uberon(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    if 'parents' in init:
      for parent_id in init['parents']:
        sql = "INSERT INTO uberon_parent (uid, parent_id) VALUES (%s, %s)"
        params = [init['uid'], parent_id]
        self._logger.debug(f"SQLpat: {sql}")
        self._logger.debug(f"SQLparams: {params}")
        with closing(self._conn.cursor()) as curs:
          try:
            curs.execute(sql, params)
          except Error as e:
            self._logger.error(f"MySQL Error in ins_uberon(): {e}")
            self._logger.error(f"SQLpat: {sql}")
            self._logger.error(f"SQLparams: {params}")
            self._conn.rollback()
            return False
    if 'xrefs' in init:
      for xref in init['xrefs']:
        if 'source' in xref:
          sql = "INSERT INTO uberon_xref (uid, db, value, source) VALUES (%s, %s, %s, %s)"
          params = [init['uid'], xref['db'], xref['value'], xref['source']]
        else:
          sql = "INSERT INTO uberon_xref (uid, db, value) VALUES (%s, %s, %s)"
          params = [init['uid'], xref['db'], xref['value']]
        self._logger.debug(f"SQLpat: {sql}")
        self._logger.debug(f"SQLparams: {params}")
        with closing(self._conn.cursor()) as curs:
          try:
            curs.execute(sql, params)
          except Error as e:
            self._logger.error(f"MySQL Error in ins_uberon(): {e}")
            self._logger.error(f"SQLpat: {sql}")
            self._logger.error(f"SQLparams: {params}")
            self._conn.rollback()
            return False
    try:
      self._conn.commit()
    except Error as e:
      self._logger.error(f"MySQL commit error in ins_uberon(): {e}")
      self._conn.rollback()
      return False
    return True

  def ins_drug_activity(self, init, commit=True):
    if 'target_id' in init and 'drug' in init and 'dcid' in init and 'has_moa' in init:
      params = [init['target_id'], init['drug'],  init['dcid'], init['has_moa']]
    else:
      self.warning(f"Invalid parameters sent to ins_drug_activity(): {init}")
      return False
    cols = ['target_id', 'drug', 'dcid', 'has_moa']
    vals = ['%s','%s','%s', '%s']
    for optcol in ['act_value', 'act_type', 'action_type', 'source', 'reference', 'smiles', 'cmpd_chemblid', 'cmpd_pubchem_cid', 'nlm_drug_info']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO drug_activity (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        if commit: self._conn.commit()
      except Error as  e:
        self._logger.error(f"MySQL Error in ins_drug_activity(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_cmpd_activity(self, init, commit=True):
    if 'target_id' in init and 'catype' in init and 'cmpd_id_in_src' in init:
      params = [init['target_id'], init['catype'], init['cmpd_id_in_src']]
    else:
      self.warning(f"Invalid parameters sent to ins_cmpd_activity(): {init}")
      return False
    cols = ['target_id', 'catype', 'cmpd_id_in_src']
    vals = ['%s','%s','%s']
    for optcol in ['cmpd_name_in_src', 'smiles', 'act_value', 'act_type', 'reference', 'pubmed_ids', 'cmpd_pubchem_cid']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO cmpd_activity (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        if commit: self._conn.commit()
      except Error as  e:
        self._logger.error(f"MySQL Error in ins_cmpd_activity(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_tinx_novelty(self, init):
    if 'protein_id' in init and 'score' in init:
      params = [init['protein_id'], init['score']]
    else:
      self.warning(f"Invalid parameters sent to ins_tinx_novelty(): {init}")
      return False
    sql = "INSERT INTO tinx_novelty (protein_id, score) VALUES (%s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tinx_novelty(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True

  def ins_tinx_disease(self, init):
    if 'doid' in init and 'name' in init:
      params = [init['doid'], init['name']]
    else:
      self.warning(f"Invalid parameters sent to ins_tinx_disease(): {init}")
      return False
    cols = ['doid', 'name']
    vals = ['%s','%s']
    for optcol in ['parent_doid', 'num_children', 'summary', 'num_important_targets', 'score']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO tinx_disease (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        tiid = curs.lastrowid
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tinx_disease(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return tiid
    
  def ins_tinx_importance(self, init):
    if 'protein_id' in init and 'disease_id' in init and 'score' in init:
      params = [init['protein_id'], init['disease_id'], init['score']]
    else:
      self.warning(f"Invalid parameters sent to ins_tinx_importance(): {init}")
      return False
    sql = "INSERT INTO tinx_importance (protein_id, disease_id, score) VALUES (%s, %s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        tiid = curs.lastrowid
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tinx_importance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return tiid
    
  def ins_tinx_articlerank(self, init):
    if 'importance_id' in init and 'pmid' in init and 'rank' in init:
      params = [init['importance_id'], init['pmid'], init['rank'],init['datalevel']]
    else:
      self.warning(f"Invalid parameters sent to ins_tinx_articlerank(): {init}")
      return False
    sql = "INSERT INTO tinx_articlerank (importance_id, pmid, `rank`,datalevel) VALUES (%s, %s, %s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tinx_articlerank(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_tiga(self, init):
    if 'protein_id' in init and 'ensg' in init and 'efoid' in init and 'trait' in init:
      params = [init['protein_id'], init['ensg'], init['efoid'], init['trait']]
    else:
      self.warning(f"Invalid parameters sent to ins_tiga(): {init}")
      return False
    cols = ['protein_id', 'ensg', 'efoid', 'trait']
    vals = ['%s','%s','%s','%s']
    for optcol in ['n_study', 'n_snp', 'n_snpw', 'geneNtrait', 'geneNstudy', 'traitNgene',
                   'traitNstudy', 'pvalue_mlog_median', 'pvalue_mlog_max', 'or_median',
                   'n_beta', 'study_N_mean', 'rcras', 'meanRank', 'meanRankScore']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO tiga (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True

  def ins_tiga_provenance(self, init):

    if 'ensg' in init and 'efoid' in init and 'study_acc' in init and 'pubmedid' in init:
      params = [init['ensg'], init['efoid'], init['study_acc'], init['pubmedid']]
    else:
      self.warning(f"Invalid parameters sent to ins_tiga_provenance(): {init}")
      return False
    cols = ['ensg', 'efoid', 'study_acc', 'pubmedid']
    vals = ['%s','%s','%s','%s']
    sql = "INSERT INTO tiga_provenance (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_generif(self,init):
    if 'protein_id' in init and 'pubmed_ids' in init and 'text' in init:
      params = (init['protein_id'], init['pubmed_ids'], init['text'])
    else:
      self.warning("Invalid parameters sent to ins_generif(): ", init)
      return False
    sql = "INSERT INTO generif (protein_id, pubmed_ids, text) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %d, %s, %s"%params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_DO(self,init):
    params = (init['doid'], init['name'], init['def'])
    sql = "INSERT INTO do (doid, name, def) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True

  def ins_Do_parent(self,init):
    params = (init['doid'], init['parent_id'])
    sql = "INSERT INTO do_parent (doid, parent_id) VALUES (%s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_Do_xref(self,init):
    params = (init['doid'], init['db'],init['value'])
    sql = "INSERT INTO do_xref (doid, db,value) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_mpo(self,init):
    params = (init['mpid'], init['parent_id'],init['name'],init['def'])
    sql = "INSERT INTO mpo (mpid,parent_id,name,def) VALUES (%s, %s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  def ins_rdo(self,init):
    params = (init['doid'], init['name'], init['def'])
    sql = "INSERT INTO rdo (doid, name, def) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_rdo_xref(self,init):
    params = (init['doid'], init['db'],init['value'])
    sql = "INSERT INTO rdo_xref (doid, db,value) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_drug_drug(self,init):
    params = (init['drugbank_id'], init['related_drug_name'],init['related_drug_description'],init['related_drugbank_id'])
    sql = "INSERT INTO drug_drug_interaction (drugbank_id, related_drug_name,related_drug_description,related_drug_drugbank_id) VALUES (%s, %s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True

  def ins_uberon(self,init):
    params = (init['uid'], init['name'],init['def'],init['comment'])
    sql = "INSERT INTO uberon (uid, name,def,comment) VALUES (%s, %s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  def ins_uberon_parent(self,init):
    params = (init['uid'], init['parent_id'])
    sql = "INSERT INTO uberon_parent (uid, parent_id) VALUES (%s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  def ins_uberon_xref(self,init):
    params = (init['uid'], init['db'], init['value'])
    sql = "INSERT INTO uberon_xref (uid, db,value) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_ortholog(self, init, commit=True):
    if 'protein_id' in init and 'taxid' in init and 'species' in init and 'symbol' in init and 'name' in init and 'sources' in init:
      cols = ['protein_id', 'taxid', 'species', 'symbol', 'name', 'sources']
      vals = ['%s','%s','%s', '%s','%s', '%s']
      params = [init['protein_id'], init['taxid'], init['species'], init['symbol'], init['name'], init['sources']]
    else:
      self.warning("Invalid parameters sent to ins_ortholog(): ", init)
      return False
    for optcol in ['db_id', 'geneid', 'mod_url']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO ortholog (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%(", ".join([str(p) for p in params])))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_ortholog(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL commit error in ins_ortholog(): %s"%str(e))
        return False
    
    return True
  
  def ins_homologene(self,init):
    if 'protein_id' in init:
      params=[init['protein_id'],init['groupid'], init['taxid']]
      sql = "INSERT INTO homologene (protein_id, groupid,taxid) VALUES (%s, %s, %s)"
    if 'nhprotein_id' in init:
      params=[init['nhprotein_id'],init['groupid'], init['taxid']]
      sql = "INSERT INTO homologene (nhprotein_id, groupid,taxid) VALUES (%s, %s, %s)"
   
    self._logger.debug("SQLpat: %s" % sql)
    self._logger.debug("SQLparams: %s, %s, %s" % tuple(params))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  def ins_ptscore(self, init, commit=True):
    if 'protein_id' in init and 'year' in init and 'score' in init:
      params = [init['protein_id'], init['year'], init['score']]
    else:
      self.warning("Invalid parameters sent to ins_ptscore(): ", init)
      return False
    sql = "INSERT INTO ptscore (protein_id, year, score) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%','.join([str(p) for p in params]))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_ptscore(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  
  def ins_gwas(self, init):
    params = [init['protein_id'], init['disease_trait'], init['snps'],
              init['pmid'], init['study'], init['context'],
              init['intergenic'], init['p_value'], init['or_beta'],
              init['cnv'], init['mapped_trait'], init['mapped_trait_uri']]
    
    sql = "INSERT INTO gwas (protein_id, disease_trait, snps,pmid,study,context,intergenic,p_value,or_beta,cnv,mapped_trait,mapped_trait_uri) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)"
    self._logger.debug(f"SQLpat: {sql}")
    self._logger.debug(f"SQLparams: {params}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tinx_importance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True
  
  def ins_compartment(self, init, commit=True):
    if 'ctype' not in init :
      self.warning("Invalid parameters sent to ins_compartment(): ", init)
      return False
    if 'protein_id' in init:
      cols = ['ctype', 'protein_id']
      vals = ['%s','%s']
      params = [init['ctype'], init['protein_id']]
    elif 'target_id' in init:
      cols = ['ctype', 'target_id']
      vals = ['%s','%s']
      params = [init['ctype'], init['target_id']]
    else:
      self.warning("Invalid parameters sent to ins_compartment(): ", init)
      return False
    for optcol in ['go_id', 'go_term', 'evidence', 'zscore', 'conf', 'url', 'reliability']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO compartment (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%", ".join([str(p) for p in params]))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_compartment(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  

  def delete_ccle(self):
    sql = "delete from expression where etype ='CCLE'"
    self._logger.debug(f"SQLpat: {sql}")
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tinx_importance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._conn.rollback()
        return False
    return True
  
  def ins_patent_count(self, init, commit=True):
    if 'protein_id' in init and 'year' in init and 'count' in init:
      params = [init['protein_id'], init['year'], init['count']]
    else:
      self.warning("Invalid parameters sent to ins_patent_count(): ", init)
      return False
    sql = "INSERT INTO patent_count (protein_id, year, count) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%','.join([str(p) for p in params]))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        if commit: self._conn.commit()
      except:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_patent_count(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  def delete_duplicate_from_lincs(self, commit=True):
      sql = "DELETE FROM lincs_dulplicate  WHERE id not IN (SELECT calc_id FROM (SELECT MAX(id) AS calc_id FROM lincs_dulplicate  GROUP BY protein_id , cellid ,pert_dcid HAVING COUNT(id) > 1) temp)"
      self._logger.debug("SQLpat: %s"%sql)
      with closing(self._conn.cursor()) as curs:
        try:
          curs.execute(sql)
          if commit: self._conn.commit()
        except:
          self._conn.rollback()
          self._logger.error("MySQL Error in ins_patent_count()")
          self._logger.error("SQLpat: %s"%sql)

          return False
      return True
  def ins_locsig(self, init, commit=True):
    if 'protein_id' in init and 'location' in init and 'signal' in init:
      cols = ['protein_id', 'location', '`signal`'] # NB. signal needs backticks in MySQL
      vals = ['%s','%s','%s']
      params = [init['protein_id'], init['location'], init['signal']]
    else:
      self.warning("Invalid parameters sent to ins_locsig(): ", init)
      return False
    for optcol in ['pmids']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO locsig (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%(", ".join([str(p) for p in params])))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_locsig(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    if commit:
      try:
        self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL commit error in ins_locsig(): %s"%str(e))
        return False
    
    return True
  
  def ins_panther_class(self, init, commit=True):
    if 'pcid' in init and 'name' in init:
      params = [init['pcid'], init['name']]
    else:
      self.warning("Invalid parameters sent to ins_panther_class(): ", init)
      return False
    cols = ['pcid', 'name']
    vals = ['%s','%s']
    for optcol in ['description', 'parent_pcids']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])# if 'treenum' in init:
    sql = "INSERT INTO panther_class (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%(", ".join([str(p) for p in params])))
    pcid = None
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        pcid = curs.lastrowid
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_panther_class(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return pcid
  
  def ins_p2pc(self, init, commit=True):
    if 'protein_id' in init and 'panther_class_id' in init:
      params = [init['protein_id'], init['panther_class_id']]
    else:
      self.warning("Invalid parameters sent to ins_p2pc(): ", init)
      return False
    sql = "INSERT INTO p2pc (protein_id, panther_class_id) VALUES (%s, %s)"
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%(", ".join([str(p) for p in params])))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_p2pc(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  
  def ins_pubmed(self, init, commit=True):
    if 'id' in init and 'title' in init:
      cols = ['id', 'title']
      vals = ['%s', '%s']
      params = [init['id'], init['title'] ]
    else:
      self.warning("Invalid parameters sent to ins_pubmed(): ", init)
      return False
    for optcol in ['journal', 'date', 'authors', 'abstract']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO pubmed (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    #self._logger.debug("SQLparams: %s"%','.join([str(p) for p in params]))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        if commit: self._conn.commit()
      except Error as e:
          self._conn.rollback()
          self._logger.error("MySQL Error in ins_pubmed(): %s"%str(e))
          self._logger.error("SQLpat: %s"%sql)
          #self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
          return False
    return True
  
  def ins_protein2pubmed(self, init, commit=True):
    if 'protein_id' in init and 'pubmed_id' in init:
      sql = "INSERT INTO protein2pubmed (protein_id, pubmed_id) VALUES (%s, %s)"
      params = [init['protein_id'], init['pubmed_id']]
    else:
      self.warning("Invalid parameters sent to ins_protein2pubmed(): ", init)
      return False
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%','.join([str(p) for p in params]))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_feature(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  
  def do_update(self, init):
    '''
    Function  : Update a single table.col with val by row id
    Arguments : A dictionary with keys table, id, col and val
    Returns   : Boolean indicating success or failure
    Example   :
    Scope     : Public
    Comments  :
    '''
    if 'table' in init and 'id' in init and 'col' in init and 'val' in init:
      params = [init['val'], init['id']]
    else:
      self.warning("Invalid parameters sent to do_update(): ", init)
      return False
    sql = 'UPDATE %s SET %s' % (init['table'], init['col'])
    sql += ' = %s WHERE id = %s'
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%", ".join([str(p) for p in params]))

    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._conn.rollback()
        msg = "MySQL Error: %s" % str(e)
        #self.error(msg)
        self._logger.error(msg)
        return False
    return True
  
  def ins_gene_attribute_type(self, init, commit=True):
    if 'name' in init and 'association' in init and 'description' in init and 'resource_group' in init and 'measurement' in init and 'attribute_group' in init and 'attribute_type' in init:
      params = [init['name'], init['association'], init['description'], init['resource_group'], init['measurement'], init['attribute_group'], init['attribute_type']]
    else:
      self.warning("Invalid parameters sent to ins_gene_attribute(): ", init)
      return False
    cols = ['name', 'association', 'description', 'resource_group', 'measurement', 'attribute_group', 'attribute_type']
    vals = ['%s','%s','%s','%s','%s','%s','%s']
    for optcol in ['pubmed_ids', 'url']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO gene_attribute_type (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%','.join([str(p) for p in params]))
    gat_it = None
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        gat_id = curs.lastrowid
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_gene_attribute_type(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return gat_id
  
  def ins_gene_attribute(self, init, commit=True):
    if 'protein_id' in init and 'gat_id' in init and 'name' in init and 'value' in init:
      params = [init['protein_id'], init['gat_id'], init['name'], init['value']]
    else:
      self.warning("Invalid parameters sent to ins_gene_attribute(): ", init)
      return False
    sql = "INSERT INTO gene_attribute (protein_id, gat_id, name, value) VALUES (%s, %s, %s, %s)"
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%','.join([str(p) for p in params]))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_gene_attribute(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  
  def ins_hgram_cdf(self, init, commit=True):
    if 'protein_id' in init and 'type' in init and 'attr_count' in init and 'attr_cdf' in init:
      params = [init['protein_id'], init['type'], init['attr_count'], init['attr_cdf']]
    else:
      self.warning("Invalid parameters sent to ins_hgram_cdf(): ", init)
      return False
    sql = "INSERT INTO hgram_cdf (protein_id, type, attr_count, attr_cdf) VALUES (%s, %s, %s, %s)"
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%(", ".join([str(p) for p in params])))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_hgram_cdf(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  
  def ins_ppi(self, init, commit=True):
    if 'ppitype' in init and 'protein1_id' in init and 'protein2_id' in init:
      params = [init['ppitype'], init['protein1_id'], init['protein2_id']]
    else:
      self.warning("Invalid parameters sent to ins_ppi(): ", init)
      return False
    cols = ['ppitype', 'protein1_id', 'protein2_id']
    vals = ['%s','%s','%s']
    for optcol in ['protein1_str', 'protein2_str', 'p_int', 'p_ni', 'p_wrong', 'evidence']:
      if optcol in init:
        cols.append(optcol)
        vals.append('%s')
        params.append(init[optcol])
    sql = "INSERT INTO ppi (%s) VALUES (%s)" % (','.join(cols), ','.join(vals))
    self._logger.debug("SQLpat: %s"%sql)
    self._logger.debug("SQLparams: %s"%','.join([str(p) for p in params]))
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, tuple(params))
        if commit: self._conn.commit()
      except Error as e:
        self._conn.rollback()
        self._logger.error("MySQL Error in ins_ppi(): %s"%str(e))
        self._logger.error("SQLpat: %s"%sql)
        self._logger.error("SQLparams: %s"%','.join([str(p) for p in params]))
        return False
    return True
  
  def ins_clinvar(self,init):
    #{'protein_id', 'clinvar_phenotype_id': ptname2id[pt], 'alleleid': int(row[0]), 'type': row[1], 'name': row[2], 'review_status': row[24], 'clinical_significance': row[6], 'clin_sig_simple': int(row[7]), 'last_evaluated': parse_date(row[8]), 'dbsnp_rs': int(row[9]), 'dbvarid': row[10], 'origin': row[14], 'origin_simple': row[15], 'assembly': row[16], 'chr': row[18], 'chr_acc': row[17], 'start': int(row[19]), 'stop': int(row[20]), 'number_submitters': int(row[25]), 'tested_in_gtr': tig, 'submitter_categories': int(row[29])}
    params = (init['protein_id'], init['clinvar_phenotype_id'], init['alleleid'],init['type'], init['name'], init['review_status'],init['clinical_significance'], init['clin_sig_simple'], init['last_evaluated'],init['dbsnp_rs'], init['dbvarid'], init['origin'],init['origin_simple'], init['assembly'], init['chr'],init['chr_acc'], init['start'], init['stop'],init['number_submitters'], init['tested_in_gtr'], init['submitter_categories'])
    sql = "INSERT INTO clinvar(protein_id, clinvar_phenotype_id, alleleid, `type`, name, review_status, clinical_significance, clin_sig_simple, last_evaluated, dbsnp_rs, dbvarid, origin, origin_simple, assembly, chr, chr_acc, `start`, stop, number_submitters, tested_in_gtr, submitter_categories) VALUES (%s, %s, %s,%s, %s, %s,%s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    #self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True

  def ins_clinvar_phenotype_xref(self,init):
    params = (init['clinvar_phenotype_id'], init['source'], init['value'])
    sql = "INSERT into clinvar_phenotype_xref (clinvar_phenotype_id,source,value) VALUES (%s, %s, %s)"
    self._logger.debug("SQLpat: %s" % sql)
    #self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql, params)
        self._conn.commit()
      except Error as e:
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._logger.error(f"SQLparams: {params}")
        self._conn.rollback()
        return False
    return True

  def ins_clinvar_phenotype(self,init):
    #params = ()
    sql = f"INSERT into clinvar_phenotype (name) VALUES ('{init['name']}')"
    self._logger.debug("SQLpat: %s" % sql)
    #self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql)
        cvpt_id = curs.lastrowid
        self._conn.commit()
      except Error as e:
        #print(e)
        #print(sql)
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._conn.rollback()
        return False
    return cvpt_id
  
  def run_sql(self,sql):
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql)
        cvpt_id = curs.lastrowid
        self._conn.commit()
      except Error as e:
        #print(e)
        #print(sql)
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._conn.rollback()
        return False
    return cvpt_id
  
  def update_disease_mondo(self,d,id):
    #params = ()
    sql = f"""update disease set mondoid="{id}" where name="{d}" """
    #print(sql)
    self._logger.debug("SQLpat: %s" % sql)
    #self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql)
        cvpt_id = curs.lastrowid
        self._conn.commit()
      except Error as e:
        print(e)
        #print(sql)
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._conn.rollback()
        return False
    return cvpt_id
  
  def update_disease_mondo_did(self,d,id):
    #params = ()
    sql = f"""update disease set mondoid="{id}" where did="{d}" """
    #print(sql)
    self._logger.debug("SQLpat: %s" % sql)
    #self._logger.debug("SQLparams: %s, %s, %s" % params)
    with closing(self._conn.cursor()) as curs:
      try:
        curs.execute(sql)
        self._conn.commit()
      except Error as e:
        print(e)
        #print(sql)
        self._logger.error(f"MySQL Error in ins_tiga_provenance(): {e}")
        self._logger.error(f"SQLpat: {sql}")
        self._conn.rollback()
        return False
    return True