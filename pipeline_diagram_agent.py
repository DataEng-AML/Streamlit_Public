
import streamlit as st
import streamlit.components.v1 as components

class PipelineDiagramAgent():
    def __init__(self, width="100%", height="250px"):
        self.width = width
        self.height = height
        self.nodes = []
        self.edges = []

    def _is_substep_selected(self, step_name, sub_step_name):
        """Check if a sub-step is selected based on session state"""
        step_key = step_name.replace(" ", "_").lower()
        
        # Map step names to their session state keys
        session_state_mapping = {
            "feature_engineering": "fe_checkbox_states",
            "missing_data": "missing_data_checkbox_states",
            "data_standardization_with_ai": "standardization_checkbox_states",
            "standardize_column_data": "column_standardization_checkbox_states",
            "inflection_ai_standard_feature_name": "inflection_standardization_checkbox_states",
            "human_ai_standard_feature_values": "human_ai_standardization_checkbox_states",
            "data_anomaly_evaluation": "anomaly_checkbox_states",
            "time_series_evaluation": "timeseries_checkbox_states",
            "data_normalisation": "normalization_checkbox_states"
        }
        
        session_key = session_state_mapping.get(step_key)
        if session_key and session_key in st.session_state:
            return st.session_state[session_key].get(sub_step_name, False)
        
        return False


    def _add_node(self, step, statuses, is_substep=False, parent_step=None):
        status = statuses.get(step, 'idle')
        
        # Check if this is a selected sub-step
        is_selected_substep = False
        if is_substep and parent_step:
            is_selected_substep = self._is_substep_selected(parent_step, step)
        
        if is_selected_substep:
            # Fill and border colour for selected sub-steps
            color = "#FFFFFF"  # light green fill
            border_color = '#228B22'  # Thick green border
            border_width = 1
        elif status == 'active':
            color = 'lightblue'
            border_color = '#1e88e5' if is_substep else None
            border_width = 1 if is_substep else 1
        elif status == 'completed':
            color = 'green'
            border_color = '#00c853' if is_substep else None
            border_width = 1 if is_substep else 1
        elif status == 'failed':
            color = 'lightcoral'
            border_color = '#d32f2f' if is_substep else None
            border_width = 1 if is_substep else 1
        else:
            color = 'lightgray'
            border_color = None
            border_width = 1
        
        # node = {
        #     "id": step,
        #     "label": step,
        #     "color": color,
        #     "shape": "box",
        #     "title": step
        # }


        if is_substep:
            node = {
                "id": step,
                "label": step,
                "color": color,
                "shape": "box",
                "title": step
            }
        else:
            # Decide fill based on status
            step_fill = (
                "#b9f6ca" if status == "completed" else   # light green
                "#e3f2fd" if status == "active" else      # light blue (optional)
                "#ffcdd2" if status == "failed" else      # light red (optional)
                "#e0e0e0"                                 # idle = grey ✅
            )

            node = {
                "id": step,
                "label": step,
                "shape": "box",
                "title": step,
                "color": {
                    "background": step_fill,
                    "border": color   # your existing strong color
                },
                "borderWidth": 2
            }


        # Add border styling for substeps
        if is_substep and border_color:
            node["color"] = {
                "background": color,
                "border": border_color
            }
            node["borderWidth"] = border_width
        
        self.nodes.append(node)


    def add_edge(self, from_node, to_node):
        self.edges.append({"from": from_node, "to": to_node})



    def render(self, show_download=True):
        import json
        
        # Serialize nodes and edges data for JavaScript
        nodes_json = json.dumps(self.nodes)
        edges_json = json.dumps(self.edges)
        
        html = f'''
        <!DOCTYPE html>
        <html>
        <head>
          <style type="text/css">
            #mynetwork {{
              width: {self.width};
              height: {self.height};
              border: 1px solid lightgray;
              cursor: pointer;
            }}
            .controls-container {{
              margin-top: 10px;
              text-align: right;
              display: flex;
              gap: 10px;
              justify-content: flex-end;
            }}
            .control-btn {{
              background-color: #4CAF50;
              color: white;
              padding: 8px 16px;
              border: none;
              border-radius: 4px;
              cursor: pointer;
              font-size: 14px;
            }}
            .control-btn:hover {{
              background-color: #45a049;
            }}
            .node-info {{
              margin-top: 10px;
              padding: 10px;
              background-color: #f0f0f0;
              border-radius: 4px;
              font-size: 14px;
              display: none;
            }}
          </style>
          <script type="text/javascript" src="https://unpkg.com/vis-network@9.1.2/dist/vis-network.min.js"></script>
          <link href="https://unpkg.com/vis-network@9.1.2/styles/vis-network.min.css" rel="stylesheet" type="text/css" />
        </head>
        <body>
          <div id="mynetwork"></div>
          <div id="nodeInfo" class="node-info"></div>
          <div class="controls-container">
            <button class="control-btn" onclick="openFullscreen()">⛶ Fullscreen + Download</button>
          </div>
          
          <script type="text/javascript">
            var nodesData = {nodes_json};
            var edgesData = {edges_json};
            
            var nodes = new vis.DataSet(nodesData);
            var edges = new vis.DataSet(edgesData);
            var container = document.getElementById('mynetwork');
            var data = {{
              nodes: nodes,
              edges: edges
            }};
            var options = {{
              physics: {{
                enabled: true,
                stabilization: true,
                barnesHut: {{
                  gravitationalConstant: -2000,
                  centralGravity: 0,
                  springLength: 150,
                  springConstant: 0.075,
                  damping: 5.5
                }}
              }},
              interaction: {{
                dragNodes: true,
                hover: true,
                selectable: true
              }},
              edges: {{
                arrows: {{
                  to: {{ enabled: true }}
                }},
                smooth: false
              }}
            }};
            var network = new vis.Network(container, data, options);
            
            // Track previously selected node for highlighting
            var previouslySelected = null;

            network.on("dragEnd", function(params) {{
              if(params.nodes.length > 0) {{
                var nodeId = params.nodes[0];
                var pos = network.getPositions([nodeId])[nodeId];
                console.log("Dragged node", nodeId, "to position", pos);
              }}
            }});
            
            // Add click handler for nodes - highlights selected node and shows info
            network.on("click", function(params) {{
              if (params.nodes.length > 0) {{
                var nodeId = params.nodes[0];
                var node = nodes.get(nodeId);
                
                // Reset previous selection
                if (previouslySelected && previouslySelected.id !== nodeId) {{
                  nodes.update(previouslySelected.id, {{ 
                    color: previouslySelected.color,
                    borderWidth: previouslySelected.borderWidth
                  }});
                }}
                
                // Highlight selected node
                var originalColor = node.color;
                nodes.update(nodeId, {{ 
                  color: {{ background: "#FFD700", border: "#FFA500" }},
                  borderWidth: 3
                }});
                
                previouslySelected = {{ 
                  id: nodeId, 
                  color: originalColor, 
                  borderWidth: node.borderWidth 
                }};
                
                // Show node info
                var infoDiv = document.getElementById("nodeInfo");
                infoDiv.innerHTML = "<strong>Selected:</strong> " + nodeId + "<br><strong>Type:</strong> " + (node.title || "Step");
                infoDiv.style.display = "block";
                
                console.log("Clicked node:", nodeId);
              }} else {{
                // Clicked on empty space - deselect
                if (previouslySelected) {{
                  nodes.update(previouslySelected.id, {{ 
                    color: previouslySelected.color,
                    borderWidth: previouslySelected.borderWidth
                  }});
                  previouslySelected = null;
                }}
                var infoDiv = document.getElementById("nodeInfo");
                infoDiv.style.display = "none";
              }}
            }});

            function openFullscreen() {{
              const fsHtml = `
                <!DOCTYPE html>
                <html>
                <head>
                  <title>Pipeline Graph - Fullscreen</title>
                  <style>
                    body {{ margin: 0; padding: 20px; background: white; font-family: Arial, sans-serif; }}
                    #fullscreenNetwork {{
                      width: 100vw;
                      height: calc(100vh - 80px);
                      border: 2px solid #444;
                      border-radius: 8px;
                    }}
                    .controls {{
                      height: 60px;
                      display: flex;
                      justify-content: flex-end;
                      gap: 10px;
                      align-items: center;
                      padding: 10px;
                      background: #16213e;
                      border-radius: 8px;
                      margin-bottom: 10px;
                    }}
                    .btn {{
                      padding: 10px 20px;
                      border: none;
                      border-radius: 4px;
                      cursor: pointer;
                      font-size: 14px;
                      font-weight: bold;
                    }}
                    .download-btn {{ background: #4CAF50; color: white; }}
                    .close-btn {{ background: #f44336; color: white; }}
                  </style>
                  <script src="https://unpkg.com/vis-network@9.1.2/dist/vis-network.min.js"><\\/script>
                </head>
                <body>
                  <div class="controls">
                    <button class="btn download-btn" onclick="downloadNetwork()">Download PNG</button>
                    <button class="btn close-btn" onclick="window.close()">Close</button>
                  </div>
                  <div id="fullscreenNetwork"></div>
                  <script>
                    var nodes = new vis.DataSet({nodes_json});
                    var edges = new vis.DataSet({edges_json});
                    var container = document.getElementById('fullscreenNetwork');
                    var data = {{ nodes: nodes, edges: edges }};
                    var options = {{
                      physics: {{ enabled: true, stabilization: true }},
                      interaction: {{ dragNodes: true, hover: true, zoomView: true }},
                      edges: {{ arrows: {{ to: {{ enabled: true }} }}, smooth: false }}
                    }};
                    var network = new vis.Network(container, data, options);
                    
                    function downloadNetwork() {{
                      var canvas = container.querySelector('canvas');
                      if (canvas) {{
                        var link = document.createElement('a');
                        link.download = 'pipeline_graph_fullscreen.png';
                        link.href = canvas.toDataURL('image/png');
                        link.click();
                      }}
                    }}
                  <\\/script>
                </body>
                </html>
              `;
              
              const blob = new Blob([fsHtml], {{ type: 'text/html' }});
              const blobUrl = URL.createObjectURL(blob);
              window.open(blobUrl, '_blank');
            }}

            function downloadNetwork() {{
              var canvasElement = container.querySelector('canvas');
              if (canvasElement) {{
                var link = document.createElement('a');
                link.download = 'pipeline_graph.png';
                link.href = canvasElement.toDataURL('image/png');
                link.click();
              }} else {{
                alert('Please wait for the graph to load completely');
              }}
            }}
          </script>
        </body>
        </html>
        '''
        components.html(html, height=int(self.height.replace("px",""))+80, scrolling=True)




    # def render(self):
    #     html = f'''
    #     <!DOCTYPE html>
    #     <html>
    #     <head>
    #       <style type="text/css">
    #         #mynetwork {{
    #           width: {self.width};
    #           height: {self.height};
    #           border: 1px solid lightgray;
    #         }}
    #       </style>
    #       <script type="text/javascript" src="https://unpkg.com/vis-network@9.1.2/dist/vis-network.min.js"></script>
    #       <link href="https://unpkg.com/vis-network@9.1.2/styles/vis-network.min.css" rel="stylesheet" type="text/css" />
    #     </head>
    #     <body>
    #       <div id="mynetwork"></div>
    #       <script type="text/javascript">
    #         var nodes = new vis.DataSet({self.nodes});
    #         var edges = new vis.DataSet({self.edges});
    #         var container = document.getElementById('mynetwork');
    #         var data = {{
    #           nodes: nodes,
    #           edges: edges
    #         }};
    #         var options = {{
    #           physics: {{
    #             enabled: true,
    #             stabilization: true,
    #             barnesHut: {{
    #               gravitationalConstant: -2000,
    #               centralGravity: 0,
    #               springLength: 150,
    #               springConstant: 0.075,
    #               damping: 5.5
    #             }}
    #           }},
    #           interaction: {{
    #             dragNodes: true,
    #             hover: true
    #           }},
    #           edges: {{
    #             arrows: {{
    #               to: {{ enabled: true }}    // This enables arrows on edges pointing to 'to' node
    #             }},
    #             smooth: false              // Optional: edges drawn without smoothing for straight lines
    #           }}
    #         }};
    #         var network = new vis.Network(container, data, options);

    #         network.on("dragEnd", function(params) {{
    #           if(params.nodes.length > 0) {{
    #             var nodeId = params.nodes[0];
    #             var pos = network.getPositions([nodeId])[nodeId];
    #             console.log("Dragged node", nodeId, "to position", pos);
    #             // Here you would implement sending this info back to Streamlit if needed
    #           }}
    #         }});
    #       </script>
    #     </body>
    #     </html>
    #     '''
    #     components.html(html, height=int(self.height.replace("px",""))+50, scrolling=True)
