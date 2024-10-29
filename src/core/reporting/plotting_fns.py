import pandas as pd

import plotly.express as px
import plotly.graph_objects as go

from core.reporting.analtyics_fns import calculate_loess

# Constants for layout_params keys
TITLE_KEY = 'title'
YAXIS_TITLE_KEY = "yaxis_title"
FONT_SIZE_KEY = "font_size"
FONT_COLOR_KEY = "font_color"
PLOT_BGCOLOR_KEY = "plot_bgcolor"
PAPER_BGCOLOR_KEY = "paper_bgcolor"
LINE_WIDTH_KEY = "line_width"
MARKER_SIZE_KEY = "marker_size"

BGCOLOR = "#0F1933"


def apply_common_layout(fig, layout_params: dict, y_max_value: float = None):
    """Applies common layout settings to the plotly figure, ensuring Y-axis tick labels are integers and automatic when range > 10."""
    fig.update_layout(
        title={
            'text': layout_params.get(TITLE_KEY, None),  # Title text
            'x': 0.5,  # Center the title
            'xanchor': 'center',  # Anchor the title at the center
            'yanchor': 'top',  # Optional: can also control vertical positioning
        },
        xaxis_title=None,
        yaxis_title=layout_params.get(YAXIS_TITLE_KEY, None),
        legend_title_text=None,
        plot_bgcolor=layout_params.get(PLOT_BGCOLOR_KEY, BGCOLOR),
        paper_bgcolor=layout_params.get(PAPER_BGCOLOR_KEY, BGCOLOR),
        font=dict(
            color=layout_params.get(FONT_COLOR_KEY, "white"),
            size=layout_params.get(FONT_SIZE_KEY, 14)
        ),
    )
    # fig.update_xaxes(tickformat="%b %Y", dtick="M2", tickangle=35, range=["2023-01-10", "2024-11-15"])
    fig.update_xaxes(tickformat="%b %Y", tickangle=35)

    # # Dynamic tick handling for Y-axis
    # if y_max_value is not None and y_max_value <= 10:
    #     # For small ranges, ensure integer ticks
    #     fig.update_yaxes(tickmode="linear", dtick=1, tickformat="d", range=[0, None])
    # else:
    #     # Allow Plotly to automatically handle the tick spacing for larger ranges
    #     fig.update_yaxes(tickmode="auto", tickformat="d", range=[0, None])

    return fig


def get_common_color_mapping():
    """Returns the common color mapping for party affiliations."""
    return {
        "Democrat": "blue",
        "Republican": "red",
        "Independent": "orange",
    }


def get_common_category_orders():
    """Returns the common category order for party affiliations."""
    return {
        "party_affiliation": ["Democrat", "Republican", "Independent"]
    }


def create_plot(df: pd.DataFrame,
                plot_type: str,
                x: str = 'published_at',
                y: str = 'count',
                layout_params=None):
    """
    Creates either a scatter or line plot based on the provided plot_type.
    - plot_type can be 'scatter' or 'line'.
    """
    if layout_params is None:
        layout_params = dict()
    if plot_type == "scatter":
        fig = px.scatter(
            df,
            x=x,
            y=y,
            color="party_affiliation",
            color_discrete_map=get_common_color_mapping(),
            category_orders=get_common_category_orders(),
        )
        fig.update_traces(marker=dict(size=layout_params.get(MARKER_SIZE_KEY, 9)))  # Use constant for marker_size

    elif plot_type == "line":
        fig = px.line(
            df,
            x="published_at",
            y="cumulative_count",
            color="party_affiliation",
            color_discrete_map=get_common_color_mapping(),
            category_orders=get_common_category_orders(),
        )
        fig.update_traces(line=dict(width=layout_params.get(LINE_WIDTH_KEY, 4)))  # Use constant for line_width

    return fig


def get_tweet_timeline_plot(df: pd.DataFrame, layout_params: dict):
    """Generates a scatter plot for tweet counts over time."""
    df = df.copy().reset_index()

    fig = create_plot(df, plot_type="scatter", layout_params=layout_params)

    # Apply common layout with parameters, yaxis_title now comes from layout_params
    fig = apply_common_layout(fig, layout_params)

    return fig


def get_tweet_cumulative_timeline_plot(df: pd.DataFrame, layout_params: dict):
    """Generates a cumulative tweet count plot over time."""
    df = df.copy().sort_values(by="published_at").reset_index()  # Sort by date
    df["cumulative_count"] = df.groupby("party_affiliation")["count"].cumsum()  # Calculate cumulative sum

    fig = create_plot(df, plot_type="line", layout_params=layout_params)

    # Apply common layout with parameters, yaxis_title now comes from layout_params
    fig = apply_common_layout(fig, layout_params)

    return fig


def get_loess_smoothed_plot(df_term: pd.DataFrame, x: str, y: str, plot_layout_params: dict, template="plotly_dark", opacity=0.5):


    # Create the scatter plot for the filtered data
    scatter_trace = go.Scatter(
        x=df_term[x],
        y=df_term[y],
        mode='markers',
        marker=dict(
            color=df_term['party_affiliation'].map({'Republican': 'red', 'Democrat': 'blue'}),
            opacity=opacity
        ),
        text=df_term['twitter_timeline_entry_text'],
        hoverinfo='text',
        showlegend=False,
    )

    fig = go.Figure()
    fig.add_trace(scatter_trace)

    # Calculate LOESS smoothed lines
    df_term_loess = df_term.groupby('party_affiliation').apply(calculate_loess, score_col=y, frac=1. / 3,
                                                               include_groups=False).reset_index()

    # Add separate LOESS lines per party affiliation
    parties = sorted(df_term_loess['party_affiliation'].unique())
    for party in parties:
        party_df = df_term_loess[df_term_loess['party_affiliation'] == party]
        line_color = 'red' if party == 'Republican' else 'blue'
        fig.add_trace(go.Scatter(
            x=party_df[x],
            y=party_df[f'{y}_loess'],
            mode='lines',
            name=f'{party}',
            line=dict(color=line_color),
            showlegend=True,
        ))

    fig = apply_common_layout(fig, plot_layout_params)

    # Update layout to remove x-axis title and adjust legend
    fig.update_layout(
        xaxis_title=None,
        yaxis_title='Sentiment',
        yaxis=dict(
            range=[-1.2, 1.2],
            tickvals=[-0.95, -0.5, 0, 0.55, 0.95],
            ticktext=['--', '-', '0', '+', '++'],
            tickfont=dict(size=14)
        ),
        showlegend=True,
        template=template,
    )

    fig.update_xaxes(gridcolor='lightgray')
    fig.update_yaxes(gridcolor='lightgray')

    return fig

