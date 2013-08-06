package org.apache.drill.outer.utils;

import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.jgraph.JGraph;
import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.JGraphModelAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class GraphVisualize {
  
  private static DirectedGraph<LogicalOperator, DefaultEdge> buildDirectedGraph(LogicalPlan plan) {
    DirectedGraph<LogicalOperator, DefaultEdge> graph = new SimpleDirectedGraph<LogicalOperator, DefaultEdge>(DefaultEdge.class);
    Collection<Edge<AdjacencyList<LogicalOperator>.Node>> edges = plan.getGraph().getAdjList().getAllEdges();    
    for(Edge<AdjacencyList<LogicalOperator>.Node> edge:edges){
      graph.addVertex(edge.getFrom().getNodeValue());
      graph.addVertex(edge.getTo().getNodeValue());
      graph.addEdge(edge.getFrom().getNodeValue(), edge.getTo().getNodeValue());
    }
    return graph;
    
  }
  
  public static void visualize(LogicalPlan plan){
    DirectedGraph<LogicalOperator, DefaultEdge> grapht = buildDirectedGraph(plan);
    // create a visualization using JGraph, via the adapter
    JGraphModelAdapter<LogicalOperator, DefaultEdge> adapter = new JGraphModelAdapter<LogicalOperator, DefaultEdge>(grapht);
    
    JGraph jgraph = new JGraph( adapter ); 
    JPanel panel = new JPanel();
    panel.setDoubleBuffered(false);
    panel.add( jgraph );
    panel.setVisible( true ); 
    panel.setEnabled( true );
    panel.addNotify();

    jgraph.setPreferredSize(new Dimension(600,500));
    jgraph.setLayout(new FlowLayout());
    
    panel.validate();
    BufferedImage img = jgraph.getImage(jgraph.getBackground(), 0);
    FileOutputStream out = null;
    try {
      out = new FileOutputStream("test.png");
      ImageIO.write(img, "png", out);
      out.flush();
      out.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();  //e:
    } catch (IOException e) {
      e.printStackTrace();  //e:
    }
  }

  public static void main(String[] args) {
    
    List<LogicalOperator> ops = new ArrayList<>();
  }
}
