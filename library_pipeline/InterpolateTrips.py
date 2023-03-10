import logging

import numpy as np
from pyinterpolate import read_txt, build_experimental_variogram, build_theoretical_variogram, kriging
import ExtractDrtTrips as edt
import pandas as pd
import SpacialOperations as sops
import logging
import tqdm

# length of interval in h E.g. 1 hour intervals --> interval = 1
# timeInterval gives the start and end of analysis. E.g. from 6am to 8pm  --> timeInterval = [06:00:00, 20:00:00]
def runAllIntervals(df,cellsDF,interval,timeInterval,hubsDf):
    logging.info('Starting interpolation of travel and wait times')
    cuts = edt.timeFrame2intervals(timeInterval,interval)
    progress = tqdm.tqdm(total=len(cuts)-1, desc='Interpolation for time interval', position=0)
    input = cellsDF.copy()
    for i in range(0,len(cuts)-1):
        progress.update(1)
        if i > 0:
            print()
            print('Interval ' + str(i+1))
            outDF = run(df, input, [cuts[i], cuts[i+1]], hubsDf)
            if len(outDF) == 0:
                pass
            else:
                outDF['Interval_start'] = [cuts[i]] * len(outDF)
                outDF['Interval_end'] = [cuts[i + 1]] * len(outDF)
                cellsDF = pd.concat([cellsDF, outDF], ignore_index=True)
        else:
            print()
            print('Interval 1')
            cellsDF = run(df, input, [cuts[i], cuts[i+1]], hubsDf)
            if len(cellsDF) == 0:
                cellsDF = input.copy()
                cellsDF.drop(df.index, inplace=True)
            else:
                cellsDF['Interval_start'] = [cuts[i]] * len(cellsDF)
                cellsDF['Interval_end'] = [cuts[i + 1]] * len(cellsDF)
    return cellsDF



def run(df,cellsDF,timeInterval,hubsDf):
    hubs = df['Hub'].unique()
    intervalStart = timeInterval[0]
    intervalEnd = timeInterval[1]
    print('Interpolation for Time interval ' + str(int(intervalStart/3600))+ ':' + str(int((intervalStart%3600)/60)) + ':00 to ' + str(int(intervalEnd/3600))+ ':' + str(int((intervalEnd%3600)/60))+':00')
    for hub in hubs:
        #Filter for current evaluated hub
        dfFiltered = df[(df['Hub'] == hub)]

        convexHullPoly = dfFiltered.unary_union.convex_hull.buffer(250)
        cellsFiltered = FilterCells2(convexHullPoly, cellsDF)

        # Filter for time interval
        df_ac_eg = dfFiltered[(dfFiltered['<leg>_dep_time'] > intervalStart) &
                        (dfFiltered['<leg>_dep_time'] < intervalEnd)][['start_x', 'start_y','end_x', 'end_y', '<leg>_trav_time', 'wait_time', 'tripType']]

        print(hub)
        # Interpolating Access Trips
        df_input = df_ac_eg[df_ac_eg['tripType'] == 'access']
        print('Interpolating Access Trips')
        print('Found ' + str(len(df_input)) + ' trips to interpolate')
        if len(df_input) < 2:
            print('Time interval has not enough entries for ' + hub)
            #continue
        else:
            typ = '<leg>_trav_time'
            dfUnique = df_input.groupby(by=['start_x', 'start_y'], as_index=False)[typ].mean()
            dfUnique.columns = ['start_x','start_y',typ]
            cellsDF = interpolate(hub,typ,dfUnique.to_numpy(),cellsDF,cellsFiltered,'ac')

            typ = 'wait_time'
            dfUnique = df_input.groupby(by=['start_x', 'start_y'], as_index=False)[typ].mean()
            dfUnique.columns = ['start_x','start_y',typ]
            cellsDF = interpolate(hub,typ,dfUnique.to_numpy(),cellsDF, cellsFiltered, 'ac')

        # Interpolating Egress Trips
        df_input = df_ac_eg[df_ac_eg['tripType'] == 'egress']
        print('Interpolating Egress Trips')
        print('Found ' + str(len(df_input)) + ' trips to interpolate')
        if len(df_input) < 2:
            print('Time interval has not enough entries for ' + hub)
            #continue
        else:
            typ = '<leg>_trav_time'
            dfUnique = df_input.groupby(by=['end_x', 'end_y'], as_index=False)[typ].mean()
            if len(dfUnique[typ].unique())<2:
                dfUnique.loc[0,typ] = dfUnique.loc[0,typ]+1
            dfUnique.columns = ['end_x', 'end_y', typ]
            cellsDF = interpolate(hub, typ, dfUnique.to_numpy(), cellsDF, cellsFiltered, 'eg')

            typ = 'wait_time'
            dfUnique = df_input.groupby(by=['end_x', 'end_y'], as_index=False)[typ].mean()
            if len(dfUnique[typ].unique())<2:
                dfUnique.loc[0,typ] = dfUnique.loc[0,typ]+1
            dfUnique.columns = ['end_x', 'end_y', typ]
            cellsDF = interpolate(hub, typ, dfUnique.to_numpy(), cellsDF, cellsFiltered, 'eg')


    return cellsDF

def interpolate(runID,typ,data,cellsDF, cellsFiltered, direction):
    unknown_point = cellsFiltered[['Centroid_X', 'Centroid_Y']].to_numpy()
    try:
        step_radius = 500  # meters
        max_range = 3000  # meters
        print('trying step: '+ str(step_radius))
        exp_semivar = build_experimental_variogram(input_array=data, step_size=step_radius, max_range=max_range)
    except:
        try:
            step_radius = 1500  # meters
            max_range = 3000  # meters
            print('trying step: ' + str(step_radius))
            exp_semivar = build_experimental_variogram(input_array=data, step_size=step_radius, max_range=max_range)
        except:
            step_radius = 3000  # meters
            max_range = 3000  # meters
            print('trying step: ' + str(step_radius))
            exp_semivar = build_experimental_variogram(input_array=data, step_size=step_radius, max_range=max_range)

    semivar = build_theoretical_variogram(experimental_variogram=exp_semivar, model_type='linear',
                                          sill=exp_semivar.variance, rang=3000)
    prediction = kriging(observations=data,
                         theoretical_model=semivar,
                         points=unknown_point,
                         how='ok',
                         no_neighbors=20)

    predictionsDF = pd.DataFrame(prediction,columns = [str(runID +'_'+typ+'_'+direction), str(runID +'_'+typ+'_'+ direction) + '_var_err', 'x','y'])
    df = cellsDF.merge(predictionsDF,left_on=['Centroid_X', 'Centroid_Y'], right_on=['x','y'], how= 'left')
    df.fillna('-999', inplace=True)
    df.drop(columns=['x','y'],inplace=True)
    return df

def cellDist2Hub(cellsDf, radius, hubLocation):
    cellX = cellsDf['Centroid_X']
    cellY = cellsDf['Centroid_Y']
    cellDist = []
    for i in range(len(cellX)):
        cellDist.append(sops.pointinCircle(radius,hubLocation,[cellX[i],cellY[i]]))
    return cellDist

def FilterCells(cellsDf, radius, hubLocation):
    cellsDf['relevant'] = cellDist2Hub(cellsDf, radius, hubLocation)
    cellsFiltered = cellsDf[cellsDf['relevant']==True]
    cellsDf.drop(columns=['relevant'], inplace=True)
    return cellsFiltered

def FilterCells2(convexHullPoly, cellsDF):
    cdf_f = cellsDF.copy()
    cdf_f['TripCatchement'] = cdf_f['geometry'].overlaps(convexHullPoly) | cdf_f['geometry'].within(convexHullPoly)
    cdf_f = cdf_f[cdf_f['TripCatchement'] == True]
    cdf_f.reset_index(inplace = True)
    return cdf_f

def getHubLocation(HubsDf,hubName):
    locationWKT = HubsDf[HubsDf['name']==hubName]['position'].values.tolist()
    coord = locationWKT[0].split(' ')
    del coord[0]
    coord[0] = coord[0].lstrip('(')
    coord[1] = coord[1].rstrip(')')
    return [float(coord[0]),float(coord[1])]




