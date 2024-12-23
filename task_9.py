import pandas as pd
import matplotlib.pyplot as plt
from pandas import DataFrame


def draw_avg_price_by_date(plotting_data: DataFrame):
    avg_price_by_date = plotting_data.groupby('date')['price'].mean()

    plt.figure(figsize=(10, 5))
    plt.ticklabel_format(style='plain')
    avg_price_by_date.plot()
    plt.title('average price by date')
    plt.xlabel('date')
    plt.ylabel('average price')

    plt.show()


def draw_cnt_by_levels(plotting_data: DataFrame):
    level_counts = plotting_data['levels'].value_counts().sort_index()
    level_percentages = (level_counts / level_counts.sum()) * 100

    plt.figure(figsize=(10, 5))
    level_percentages.plot(kind='bar')
    plt.title('Percentage of houses by levels')
    plt.xlabel('levels')
    plt.ylabel('percentage')
    plt.ylim(0, 25)

    plt.show()


def draw_id_regions(plotting_data: DataFrame):
    region_counts = plotting_data['id_region'].value_counts().sort_index()

    plt.figure(figsize=(8, 8))
    plt.pie(region_counts, labels=region_counts.index, labeldistance=1.05)
    plt.title('Number of houses by id_region')

    plt.show()


def draw_correlation_for_area(plotting_data: DataFrame):
    filtered_data = plotting_data[plotting_data['kitchen_area'] >= 0]

    plt.figure(figsize=(10, 5))
    plt.scatter(filtered_data['area'], filtered_data['kitchen_area'], color='blue', alpha=0.6, edgecolors='w', linewidth=0.5)
    plt.title('Correlation between area and kitchen_area')
    plt.xlabel('area')
    plt.ylabel('kitchen_area')
    plt.grid(True)

    correlation = filtered_data['area'].corr(filtered_data['kitchen_area'])
    plt.text(0, 175, f'Correlation: {correlation:.3f}', fontsize=12)

    plt.show()


def draw_avg_price_by_level(plotting_data: DataFrame):
    avg_price_by_level = plotting_data.groupby('level')['price'].mean().sort_index()

    plt.figure(figsize=(10, 5))
    plt.ticklabel_format(style='plain')
    avg_price_by_level.plot(kind='bar')
    plt.title('average price by level')
    plt.xlabel('level')
    plt.ylabel('average price')

    plt.show()


def draw_number_of_houses_by_rooms(plotting_data: DataFrame):
    rooms_counts = plotting_data['rooms'].value_counts().sort_index()

    plt.figure(figsize=(8, 8))
    plt.pie(rooms_counts, labels=rooms_counts.index, labeldistance=1.05, autopct='%1.1f%%')
    plt.title('Number of houses by rooms count')

    plt.show()


def draw_avg_price_by_multiple_params(plotting_data: DataFrame):
    filtered_data = plotting_data[plotting_data['rooms'] >= 0]
    avg_price = filtered_data.groupby(['level', 'rooms'])['price'].mean().reset_index()

    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(121, projection='3d')
    plt.ticklabel_format(style='plain')
    ax.scatter(avg_price['level'], avg_price['rooms'], avg_price['price'], c=avg_price['price'])
    ax.set_xlabel('level')
    ax.set_ylabel('rooms')
    ax.set_zlabel('average price')
    ax.set_title('average price by level and rooms')

    plt.show()


# Step 9
path = './data/plotting_data.csv'
plotting_data = pd.read_csv(path)

draw_avg_price_by_date(plotting_data)
draw_cnt_by_levels(plotting_data)
draw_id_regions(plotting_data)
draw_correlation_for_area(plotting_data)
draw_avg_price_by_level(plotting_data)
draw_number_of_houses_by_rooms(plotting_data)
draw_avg_price_by_multiple_params(plotting_data)
