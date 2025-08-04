import pandas as pd

def sort_relation(rel, C, sort_by_time=False, P=False) -> pd.DataFrame:
  '''
  Input: rel = relation, C = equality attributes (should be a list), sort_by_time = True if order by P.B else False
  Output: rel order by C and rel.B
  '''
  # extract the beginning of the time periods
  if P == True:
    rel['P.B'] = rel['P'].apply(lambda x: x.start)
    rel['P.E'] = rel['P'].apply(lambda x: x[-1])

  # sort the dataframe by C and B
  col_to_sort = [ e for e in C ]
  if sort_by_time == True:
    col_to_sort.append('P.B')
  rel.sort_values(by=col_to_sort, inplace=True, ignore_index=True)

  return rel


def relation_next_tuple(rel, idx):
  '''r <- next(r)'''
  idx += 1
  return rel.iloc[idx], idx


def rmj_inequality_RS(rel_r, rel_r_prime, C):
  '''
  Input: relations r & r', 
         equality attributes C (list if you use the function 'sort_relations' and multiple attributes, else string)
  Output: result of r ⋈ r'
  '''
  # initialisation (for Python)
  join_result = []

  # read the first tuple
  r = rel_r.iloc[0]
  r_prime = rel_r_prime.iloc[0]

  # as long as the two relations have not yet ben fully read, one of the following steps is executed
  i, j = 0, 0
  while i < len(rel_r):
    try:
      if (r[C] <= r_prime[C]).all():
        i += 1
        r = rel_r.iloc[i]
      else:
        sscanidx = 0
        sscan = rel_r_prime.iloc[sscanidx]
        while (sscanidx <= j):
          if (r['P.B'] <= sscan['P.B']) and (sscan['P.B'] < r['P.E']):
            join_result.append((r['idx'], sscan['idx']))
          sscanidx += 1
          sscan = rel_r_prime.iloc[sscanidx]
        j += 1
        r_prime = rel_r_prime.iloc[j]
    except IndexError:
      break
  return join_result


def rmj_inequality_SR(rel_r_prime, rel_r, C):
  '''
  Input: relations r' & r, 
         equality attributes C (list if you use the function 'sort_relations' and multiple attributes, else string)
  Output: result of r' ⋈ r
  '''
  # initialisation
  join_result = []

  # read the first tuple
  r = rel_r.iloc[0]
  r_prime = rel_r_prime.iloc[0]

  # as long as the two relations have not yet ben fully read, one of the following steps is executed
  i, j = 0, 0
  while i < len(rel_r):
    try:
      if (r[C] <= r_prime[C]).all():
        i += 1
        r = rel_r.iloc[i]
      else:
        sscanidx = 0
        sscan = rel_r_prime.iloc[sscanidx]
        while (sscanidx <= j):
          if (sscan['P.B'] < r['P.B']) and (r['P.B'] < sscan['P.E']):
            join_result.append((r['idx'], sscan['idx']))
          sscanidx += 1
          sscan = rel_r_prime.iloc[sscanidx]
        j += 1
        r_prime = rel_r_prime.iloc[j]
    except IndexError:
      break
  return join_result


def OMJ_inequality(rel_r, rel_r_prime, C):
  '''
  Input: relation r with schema R, relation s with schema S, 
         equality attributes C (list if you use the function 'sort_relations' and multiple attributes, else string)  
  Output: Result of r ⋈_r.C=s.C ∧ Ov(r.P,s.P) s
  '''
  # two range-merge joins  are executed with the two input-relations swapped
  z1 = rmj_inequality_RS(rel_r, rel_r_prime, C)
  z2 = rmj_inequality_SR(rel_r_prime, rel_r, C)

  return z1 + z2