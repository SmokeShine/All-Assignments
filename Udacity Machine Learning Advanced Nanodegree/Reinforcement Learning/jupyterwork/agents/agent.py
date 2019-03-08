# TODO: your agent here!

import numpy as np
import task as Task
import random
from collections import deque, namedtuple
from agents.critic import Critic
from agents.actor import Actor
from agents.ounoise import OUNoise
import pdb

class ReplayBuffer():
    def __init__(self, buffer_size, batch_size):
        self.memory = deque(maxlen=buffer_size)
        self.batch_size = batch_size
        self.experience = namedtuple("Experience", field_names = ["state","action","reward","next_state","done"])
        
    def add(self,state,action,reward,next_state,done):
        e = self.experience(state,action,reward,next_state,done)
        self.memory.append(e)
        
    def sample(self,batch_size=64):
        #         pdb.set_trace()
        return random.sample(self.memory,k=batch_size)
    
    def __len__(self):
        return len(self.memory)
class Agent():
    def __init__(self,task):
        self.task = task
        self.state_size = task.state_size
        self.action_size = task.action_size
        self.action_high = task.action_high
        self.action_low = task.action_low
        self.actor_local = Actor(self.state_size,self.action_size,self.action_high,self.action_low)
        self.actor_target = Actor(self.state_size,self.action_size,self.action_high,self.action_low)
        self.critic_local = Critic(self.state_size,self.action_size)
        self.critic_target = Critic(self.state_size,self.action_size)
        self.actor_target.model.set_weights(self.actor_local.model.get_weights())
        self.critic_target.model.set_weights(self.critic_local.model.get_weights())
        self.exploration_mu = 0
        self.exploration_theta = 0.25
        self.exploration_sigma = 0.3
        self.noise = OUNoise(self.action_size, self.exploration_mu, self.exploration_theta, self.exploration_sigma)
        self.buffer_size = 100000
        self.batch_size = 128
        self.memory = ReplayBuffer(self.buffer_size,self.batch_size)
        self.gamma = 0.9 
        self.tau = 0.1 
        self.total_reward = 0
        self.count = 0
        self.score = 0
        self.best_score = -np.inf
        self.reset_episode()
    def reset_episode(self):
        self.noise.reset()
        state = self.task.reset()
        #         pdb.set_trace()
        self.last_state = state
        return state    
    def step(self,action,reward,next_state,done):
        #         pdb.set_trace()
        self.total_reward += reward
        self.count += 1
        self.memory.add(self.last_state,action,reward,next_state,done)
        if len(self.memory) > self.batch_size:
            #         pdb.set_trace()
            experiences = self.memory.sample()
            self.learn(experiences)
        self.last_state = next_state
    def act(self,states):
        #         pdb.set_trace()
        state = np.reshape(states, [-1,self.state_size])
        action = self.actor_local.model.predict(state)[0]
        return list(action + self.noise.sample())
    
    def learn(self,experiences):
        #         pdb.set_trace()
        self.score = self.total_reward / float(self.count) if self.count else 0.0
        states = np.vstack([e.state for e in experiences if e is not None]) 
        actions = np.array([e.action for e in experiences if e is not None]).astype(np.float32).reshape(-1,self.action_size)
        rewards = np.array([e.reward for e in experiences if e is not None]).astype(np.float32).reshape(-1,1)
        dones = np.array([e.done for e in experiences if e is not None]).astype(np.uint8).reshape(-1,1)
        next_states = np.vstack([e.next_state for e in experiences if e is not None])
        next_actions = self.actor_target.model.predict_on_batch(next_states)
        Q_targets_next = self.critic_target.model.predict_on_batch([next_states,next_actions])
        Q_targets = rewards + self.gamma*Q_targets_next*(1 - dones)
        self.critic_local.model.train_on_batch(x=[states,actions],y=Q_targets)
        action_gradients = np.reshape(self.critic_local.get_action_gradients([states,actions,0]),(-1,self.action_size))
        self.actor_local.train_fn([states,action_gradients,1])  # custom train function
        self.soft_update(self.actor_local.model,self.actor_target.model)
        self.soft_update(self.critic_local.model,self.critic_target.model)
    def soft_update(self,local_model,target_model):
        #         pdb.set_trace()
        local_weights = np.array(local_model.get_weights())
        target_weights = np.array(target_model.get_weights())
        assert len(local_weights)==len(target_weights), "Local and target model parameters must have the same size"
        new_weights = self.tau*local_weights + (1 - self.tau)*target_weights
        target_model.set_weights(new_weights)