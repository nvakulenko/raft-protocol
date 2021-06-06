package ua.edu.ucu.ds;


import ua.edu.ucu.RaftProtocolGrpc;

public class FollowerNode {

  // node replication status: prevLogIndex, prevLogTerm

  private Integer nodeId;
  private String url;
  public RaftProtocolGrpc.RaftProtocolBlockingStub rpcClient;


  public FollowerNode(String url) {
    this.url = url;
  }

  // TODO look how to send messages to a client

}
