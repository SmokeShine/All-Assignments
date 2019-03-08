from collections import deque
import sys
import math
import numpy as np

import pdb

def interact(env, agent, num_episodes=20000, window=100):
    """ Monitor agent's performance.
    
    Params
    ======
    - env: instance of OpenAI Gym's Taxi-v1 environment
    - agent: instance of class Agent (see Agent.py for details)
    - num_episodes: number of episodes of agent-environment interaction
    - window: number of episodes to consider when calculating average rewards

    Returns
    =======
    - avg_rewards: deque containing average rewards
    - best_avg_reward: largest value in the avg_rewards deque
    """
    # initialize average rewards
    alpha=0.01
    gamma=1.0
    #num_episodes=1
    #Q = defaultdict(lambda: np.zeros(env.nA))
    # env does not have env.nA and that's why we used agent nA
#    pdb.set_trace()    
    avg_rewards = deque(maxlen=num_episodes)
    # initialize best average reward
    best_avg_reward = -math.inf
    # initialize monitor for most recent rewards
    samp_rewards = deque(maxlen=window)
    # for each episode
    for i_episode in range(1, num_episodes+1):
        # begin the episode
        state = env.reset()
        # initialize the sampled reward
        samp_reward = 0
        while True:
            # agent selects an action
            action = agent.select_action(state,i_episode) #selects - does not move , added i_episode parameter
            # agent performs the selected action
            #env.render()
            next_state, reward, done, _ = env.step(action)
            # agent performs internal updates based on sampled experience
            agent.step(state, action, reward, next_state, done,i_episode) #needs to return a new state
            #so it selected on action based on policy and then does a move
            # update the sampled reward
            samp_reward += reward
            
            # update the state (s <- s') to next time step
            state = next_state
            if done:
              #pdb.set_trace()
              # update TD estimate of Q
              #env.render()
              #agent.Q[state][action] = agent.Q[state][action] + (alpha * (reward + (gamma * 0) - agent.Q[state][action]))
              #update_Q(agent,state,action,agent.Q[state][action], 0, reward, alpha, gamma)
              # save final sampled reward
              samp_rewards.append(samp_reward)
              break
        if (i_episode >= 100):
            # get average reward from last 100 episodes
            avg_reward = np.mean(samp_rewards)
            # append to deque
            avg_rewards.append(avg_reward)
            # update best average reward
            if avg_reward > best_avg_reward:
                best_avg_reward = avg_reward
        # monitor progress
        print("\rEpisode {}/{} || Best average reward {}".format(i_episode, num_episodes, best_avg_reward), end="")
        sys.stdout.flush()
        # check if task is solved (according to OpenAI Gym)
        if best_avg_reward >= 9.7:
            print('\nEnvironment solved in {} episodes.'.format(i_episode), end="")
            break
        if i_episode == num_episodes: print('\n')
    return avg_rewards, best_avg_reward
def update_Q(Qsa, Qsa_next, reward, alpha, gamma):
 # pdb.set_trace()
  return Qsa + (alpha * (reward + (gamma * Qsa_next) - Qsa))
def epsilon_greedy_probs(agent, Q_s, i_episode, eps=None):
  #pdb.set_trace()
  epsilon = 1.0 / i_episode
  if eps is not None:
    epsilon = eps
  policy_s = np.ones(agent.nA) * epsilon / agent.nA
  policy_s[np.argmax(Q_s)] = 1 - epsilon + (epsilon / agent.nA)
  return policy_s
  