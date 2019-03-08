import numpy as np
from collections import defaultdict
import pdb
#https://www.oreilly.com/learning/introduction-to-reinforcement-learning-and-openai-gym

'''
    rendering:
    - blue: passenger
    - magenta: destination
    - yellow: empty taxi
    - green: full taxi
    - other letters: locations
   nS = 500
        nR = 5
        nC = 5 
        (Pdb) env.env.s =49
		(Pdb) env.render() - use these to understand the environment
  '''
class Agent:
	
    def __init__(self, nA=6):
        """ Initialize agent.

        Params
        ======
        - nA: number of actions available to the agent
        """
        self.nA = nA
        self.Q = defaultdict(lambda: np.zeros(self.nA))
    def select_action(self, state,i_episode):
#        pdb.set_trace()
        """ Given the state, select an action.
        Params
        ======
        - state: the current state of the environment

        Returns
        =======
        - action: an integer, compatible with the task's action space
        """
        #using algorithm provided in
        # https://github.com/allanbreyes/gym-solutions/blob/master/taxi/q_learning.py
        alpha=0.01
        gamma=0.15
        epsilon=0.5
        
        # how to define policy
        policy_s=epsilon_greedy_probs(self, self.Q[state], i_episode)
        # with env, we dont have certain attributes
        # i_episode is not defined here - we will use a constant value for epsilon instead of 1.0 / i_episode
        return np.random.choice(np.arange(self.nA), p=policy_s) # took a random action based on policy

    def step(self, state, action, reward, next_state, done,i_episode):
#      pdb.set_trace()
      """ Update the agent's knowledge, using the most recently sampled tuple.

        Params
        ======
        - state: the previous state of the environment
        - action: the agent's previous choice of action
        - reward: last reward received
        - next_state: the current state of the environment
        - done: whether the episode is complete (True or False)
      """
      if not done:
        # get epsilon-greedy action probabilities
        policy_s = epsilon_greedy_probs(self, self.Q[next_state], i_episode)

        # Q to current state, Q of next state,rewards (and "NO" future rewards)
        # this is the error line
        alpha=0.01
        gamma=1.0
        next_action = np.random.choice(np.arange(self.nA), p=policy_s)
        self.Q[state][action] = update_Q(self,state,action,self.Q[state][action], np.max(self.Q[next_state][next_action]), 
                                      reward, alpha, gamma)
        # S <- S'
        state = next_state
        # A <- A'
        action = next_action
      #self.Q[state][action] += 1
        
def epsilon_greedy_probs(env, Q_s, i_episode, eps=None):
#  pdb.set_trace()
  epsilon = 1.0 / i_episode
  if eps is not None:
    epsilon = eps
  policy_s = np.ones(env.nA) * epsilon / env.nA
  policy_s[np.argmax(Q_s)] = 1 - epsilon + (epsilon / env.nA)
  return policy_s

def update_Q(Agent,state,action,Qsa, Qsa_next, reward, alpha, gamma):
  #pdb.set_trace()
  Agent.Q[state][action] = Qsa + (alpha * (reward + (gamma * Qsa_next) - Qsa))
  #return Qsa + (alpha * (reward + (gamma * Qsa_next) - Qsa))
