from src.modified_omj import relation_next_tuple

def bf_iejoin(rel_r, X, Y, rel_r_prime, X_prime, Y_prime, op1, op2) -> list:
  '''
  A naive brute force based inequality join algorithm
  
  Input:
  - relational tables rel_r and rel_r_prime of sizes m and n resp.
  - query Q with 2 join predicates r.X op1 r'.X' and r.Y op2 r'.Y'

  Output:
  - Result of r ⋈(r.X op1 r'.X') ∧ (r.Y op2 r'.Y') r' (i.e., a list of tuple pairs (r_i, r'_j))
  '''
  assert op1 in ['lt', 'le', 'gt', 'ge']
  assert op2 in ['lt', 'le', 'gt', 'ge']
  assert 'idx' in rel_r.columns # for convenience in this algorithm

  # intialisation
  operators = {'lt': (lambda x, y: x < y), 'le': (lambda x, y: x <= y),
               'gt': (lambda x, y: x > y), 'ge': (lambda x, y: x >= y)}
  join_result = []

  i = 0
  while i < len(rel_r):
    r = rel_r.iloc[i]
    j = 0
    r_prime = rel_r_prime.iloc[0]
    while j < len(rel_r_prime) and (r.idx > r_prime.idx):
      if operators[op1](r[X], r_prime[X_prime]) and operators[op2](r[Y], r_prime[Y_prime]):
        join_result.append((r['idx'], r_prime['idx']))
      r_prime, j = relation_next_tuple(rel_r_prime, j)
    i += 1
  return join_result