import pandas as pd
import numpy as np

def get_permutation_array(L1, L2):
  '''return corresponding positions of elements of L1 in L2'''
  # Extract the indexes of L1 and L2
  L1_index = L1.index.to_list()
  L2_index = L2.index.to_list()

  # Create a dictionary mapping tuples and indexes of L1
  index_map = {val: i for i, val in enumerate(L1_index)}
  # Create a permutation array of elements of L2 based on how they are arranged in L1 (index_map)
  P = [ index_map[val] for val in L2_index ]
  return P


def get_offset_array_op1(L1, L1_prime, op1) -> list:
  '''returns an offset array by comparing L1 and L1' based on op1'''
  # initialisation
  operators = {'lt': (lambda x,y: x < y), 'le': (lambda x,y: x <= y),
               'gt': (lambda x,y: x > y), 'ge': (lambda x,y: x >= y)}
  assert op1 in ['lt', 'le', 'gt', 'ge']
  O1 = []

  for l1 in L1:
    for idx, l1_prime in enumerate(L1_prime):
      if operators[op1](l1, l1_prime) == True:
        O1.append(idx)
        break
      elif operators[op1](l1, l1_prime) == False and idx == len(L1_prime)-1:
        O1.append(idx+1)
        break
  return O1


def get_offset_array_op2(L2, L2_prime, op2) -> list:
  '''returns an offset array by comparing L2 and L2' based on op2'''
  # initialisation
  operators = {'lt': (lambda x,y: x < y), 'le': (lambda x,y: x <= y),
               'gt': (lambda x,y: x > y), 'ge': (lambda x,y: x >= y)}
  assert op2 in ['lt', 'le', 'gt', 'ge']
  O2 = []

  for l2 in L2:
    for idx, l2_prime in enumerate(L2_prime):
      # add the index of the first False to O2
      if operators[op2](l2, l2_prime) == False:
        O2.append(idx-1)
        break
      # if all True
      elif operators[op2](l2, l2_prime) == True and idx == len(L2_prime)-1:
        O2.append(idx)
        break
  return O2


def iejoin(T, X, Y, T_prime, X_prime, Y_prime, op1, op2, id1, id2):
  '''
  Inequality join with two inequality predicates (T.X op1 T'.X', T.Y op2 T'.Y')
  involving two relations T and T'
  '''
  # let L1 (resp. L2) be the array of X (resp. Y) in T
  L1, L2 = T[X], T[Y]
  # let L1' (resp. L2') be the array of X' (resp. Y') in T'
  L1_prime, L2_prime = T_prime[X_prime], T_prime[Y_prime]

  assert op1 in ['gt', 'ge', 'lt', 'le']
  assert op2 in ['gt', 'ge', 'lt', 'le']

  # if (op1 in {>, >=}): sort L1, L1' in DESC order
  if op1 in ['gt', 'ge']:
    L1 = L1.sort_values(ascending=False)
    L1_prime = L1_prime.sort_values(ascending=False)
  # else if (op1 in {<, <=}): sort L1, L1' in ASC order
  elif op1 in ['lt', 'le']:
    L1 = L1.sort_values(ascending=True)
    L1_prime = L1_prime.sort_values(ascending=True)
  # if (op2 in {>, >=}): sort L2, L2' in ASC order
  if op2 in ['gt', 'ge']:
    L2 = L2.sort_values(ascending=True)
    L2_prime = L2_prime.sort_values(ascending=True)
  # else if (op2 in {<, <=}): sort L2, L2' in DESC order
  elif op2 in ['lt', 'le']:
    L2 = L2.sort_values(ascending=False)
    L2_prime = L2_prime.sort_values(ascending=False)
  
  # compute the permutation array P of L2 w.r.t. L1
  P = get_permutation_array(L1, L2)
  # compute the permutation array P' of L2' w.r.t. L1'
  P_prime = get_permutation_array(L1_prime, L2_prime)

  # compute the offset array O1 of L1 w.r.t. L1'
  O1 = get_offset_array_op1(L1, L1_prime, op1)
  # compute the offset array O2 of L2 w.r.t. L2'
  O2 = get_offset_array_op2(L2, L2_prime, op2)

  # initialise a bit-array B' (|B'| = n), and set all bits to 0
  m, n = len(T), len(T_prime)
  B_prime = np.zeros(n)
  # initialise join_result as an empty list for tuple pairs
  join_result = []
  # sequentially scan the permutation array from left to right (lines 15-22)
  for i in range(m):
    # sets all bits for those t in T' whose Y' values are smaller than the Y value of the current tuple in T
    off2 = O2[i]
    for j in range(min(off2+1, len(L2_prime))):
      B_prime[P_prime[j]] = 1
    # uses the other offset array to find those tuples in T' that also satisfy the first join condition
    off1 = O1[P[i]]
    for k in range(off1, n):
      if B_prime[k] == 1:
        left = T.iloc[L2.index[i]][id1]
        right = T_prime.iloc[L1_prime.index[k]][id2]
        join_result.append((left, right))
        # join_result.append((L2.iloc[i], L1_prime.iloc[k])) # original algo.
  # returns all join results
  return join_result



def iejoin_modified(rel_r, rel_r_prime, id):
  '''
  Input: relations r & r'
         tuple elements id to output
  Output: result of r ⋈ r' with three inequality predicates
  
  Inequality join with two inequality predicates 
  (T.X op1 T'.X', T.Y op2 T'.Y') + r.idx > r'.idx
  involving two relations T and T'
  '''
  # let L1 (resp. L2) be the array of X (resp. Y) in T
  L1, L2, L3 = rel_r['P.B'], rel_r['P.E'], rel_r['idx']
  # let L1' (resp. L2') be the array of X' (resp. Y') in T'
  L1_prime, L2_prime, L3_prime = rel_r_prime['P.E'], rel_r_prime['P.B'], rel_r_prime['idx']

  # sort L1, L1' in ASC order (as op1 in {<, <=})
  L1 = L1.sort_values(ascending=True)
  L1_prime = L1_prime.sort_values(ascending=True)
  # sort L2, L2' in ASC order (as op2 in {>, >=})
  L2 = L2.sort_values(ascending=True)
  L2_prime = L2_prime.sort_values(ascending=True)
  ########## METHOD 2: store the index of L2 and L1_prime as the 2nd inner for loop goes thru them -- worked! ############
  L3 = L2.index
  L3_prime = L1_prime.index

  # compute the permutation array P of L2 w.r.t. L1
  P = get_permutation_array(L1, L2)
  # compute the permutation array P' of L2' w.r.t. L1'
  P_prime = get_permutation_array(L1_prime, L2_prime)

  # compute the offset array O1 of L1 w.r.t. L1'
  O1 = get_offset_array_op1(L1, L1_prime, 'lt')
  # compute the offset array O2 of L2 w.r.t. L2'
  O2 = get_offset_array_op2(L2, L2_prime, 'gt')

  # initialise a bit-array B' (|B'| = n), and set all bits to 0
  m, n = len(rel_r), len(rel_r_prime)
  # B = np.zeros(m)
  B_prime = np.zeros(n)
  # initialise join_result as an empty list for tuple pairs
  join_result = []
  # sequentially scan the permutation array from left to right (lines 15-22)
  for i in range(m):
    # sets all bits for those t in T' whose Y' values are smaller than the Y value of the current tuple in T
    off2 = O2[i]
    for j in range(min(off2+1, len(L2_prime))):
      B_prime[P_prime[j]] = 1
    # uses the other offset array to find those tuples in T' that also satisfy the first join condition
    off1 = O1[P[i]]
    for k in range(off1, n):
      if B_prime[k] == 1 and (L3[i] > L3_prime[k]): # r.idx > r'.idx
        left = rel_r.iloc[L2.index[i]][id]
        right = rel_r_prime.iloc[L1_prime.index[k]][id]
        join_result.append((left, right))
  # returns all join results
  return join_result