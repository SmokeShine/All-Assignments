from agent import Agent
from monitor import interact
import gym
import numpy as np
import pdb 
env = gym.make('Taxi-v2')
agent = Agent() # initialized Q and the number of actions agent can take
#pdb.set_trace()
avg_rewards, best_avg_reward = interact(env, agent)
