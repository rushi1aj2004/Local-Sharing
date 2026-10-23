
#include <omnetpp.h>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstdint>

using namespace omnetpp;

class OLT : public cSimpleModule
{
  private:
    cMessage *scheduleMsg = nullptr;

    enum class SchedulerPhase
    {
        INITIAL_DISCOVERY,
        INITIAL_LOCAL_SHARING,
        STEADY_STATE
    };

    SchedulerPhase phase = SchedulerPhase::INITIAL_DISCOVERY;

    int numONUs = 0;
    bool initialScheduleDone = false;

    SimTime horizonUP = SIMTIME_ZERO;
    SimTime horizonLS = SIMTIME_ZERO;

    SimTime maxCycleTime;
    SimTime guardTime;

    double dataRate = 0;
    double oltToRNDelay = 0;

    long maxGrantBytes = 0;
    long reportSizeBytes = 0;
    long packetSizeBytes = 0;

    std::vector<long> queueBytes;
    std::vector<double> rnToONUDelays;

    std::vector<bool> initialReportReceived;
    int initialReportsRemaining = 0;

    int initialHostId = -1;
    std::vector<int> initialLSGroup;

    void sendGrantMessage(
        int onuId,
        long grantBytes,
        int mode,
        int hostId,
        double lsDuration,
        SimTime delayAfterGrant);

    void sendGrantForONU(int onuId, long grantBytes);

    void selectInitialLocalSharingGroup();
    void scheduleInitialLocalSharing();
    

    long alignedGrantBytes(long bytes) const;

  protected:
    virtual void initialize() override;
    virtual void handleMessage(cMessage *msg) override;
    virtual void finish() override;
};

Define_Module(OLT);


static SimTime secondsToSimTime(double seconds)
{
    constexpr double TICKS_PER_SECOND = 1e12;

    int64_t ticks = static_cast<int64_t>(
        std::llround(seconds * TICKS_PER_SECOND)
    );

    return SimTime(ticks, SIMTIME_PS);
}


long OLT::alignedGrantBytes(long bytes) const
{
    if (bytes <= 0)
        return 0;

    bytes = std::min(bytes, maxGrantBytes);
    bytes -= bytes % packetSizeBytes;

    return std::max(0L, bytes);
}


void OLT::initialize()
{
    scheduleMsg = new cMessage("InitialSchedule");

    cModule *network = getParentModule();

    numONUs = par("numONUs").intValue();

    if (numONUs < 6)
        throw cRuntimeError(
            "Initial Local Sharing requires at least 6 ONUs"
        );

    maxCycleTime = par("maxCycleTime");
    guardTime = par("guardTime");

    reportSizeBytes = par("reportSizeBytes").intValue();

    dataRate = network->par("dataRate").doubleValue();

    oltToRNDelay =
        network->par("del_OLTtoRN").doubleValue();

    if (dataRate <= 0 ||
        reportSizeBytes <= 0 ||
        guardTime < SIMTIME_ZERO ||
        maxCycleTime <= SIMTIME_ZERO)
    {
        throw cRuntimeError("Invalid OLT scheduling parameters");
    }

    rnToONUDelays.resize(numONUs);

    cModule *firstONU = network->getSubmodule("onu", 0);

    long packetSizeBits =
        firstONU->par("packetSize").intValue();

    if (packetSizeBits <= 0 || packetSizeBits % 8 != 0)
        throw cRuntimeError(
            "packetSize must be a positive multiple of 8 bits"
        );

    packetSizeBytes = packetSizeBits / 8;

    for (int i = 0; i < numONUs; i++)
    {
        cModule *onuModule = network->getSubmodule("onu", i);

        rnToONUDelays[i] =
            onuModule->par("rnToONUDelay").doubleValue();

        EV_INFO << "ONU " << i
                << " per-ONU delay = "
                << rnToONUDelays[i] * 1e6
                << " us"
                << endl;
    }

    // Calculate a conservative normal grant size.
    double reportTxTime =
        reportSizeBytes * 8.0 / dataRate;

    double usableDataTime =
        maxCycleTime.dbl()
        - numONUs * reportTxTime
        - (numONUs - 1) * guardTime.dbl();

    if (usableDataTime <= 0)
        throw cRuntimeError("Cycle time is too short");

    maxGrantBytes = static_cast<long>(
        std::floor(
            dataRate * usableDataTime / (8.0 * numONUs)
        )
    );

    maxGrantBytes -= maxGrantBytes % packetSizeBytes;

    if (maxGrantBytes < packetSizeBytes)
        throw cRuntimeError("Calculated grant is too small");

    queueBytes.assign(numONUs, 0);
    initialReportReceived.assign(numONUs, false);
    initialReportsRemaining = numONUs;

    horizonUP = SIMTIME_ZERO;
    horizonLS = SIMTIME_ZERO;

    phase = SchedulerPhase::INITIAL_DISCOVERY;

    EV_INFO << "\n=== OLT CONFIGURATION ==="
            << "\nNumber of ONUs = " << numONUs
            << "\nData rate = " << dataRate
            << " bit/s"
            << "\nMaximum grant = " << maxGrantBytes
            << " bytes"
            << "\nREPORT size = " << reportSizeBytes
            << " bytes"
            << "\nGuard time = " << guardTime
            << endl;

    scheduleAt(SIMTIME_ZERO, scheduleMsg);
}


// Create and send a GATE. Timing and reservations are calculated
// by the caller so LS and UP paths can be scheduled independently.
void OLT::sendGrantMessage(
    int onuId,
    long grantBytes,
    int mode,
    int hostId,
    double lsDuration,
    SimTime delayAfterGrant)
{
    if (onuId < 0 || onuId >= numONUs)
        throw cRuntimeError("Invalid ONU ID in GATE");

    if (grantBytes < 0 || delayAfterGrant < SIMTIME_ZERO)
        throw cRuntimeError("Invalid GATE grant or delay");

    cMessage *grant = new cMessage("Grant");

    grant->addPar("onuId");
    grant->par("onuId").setLongValue(onuId);

    grant->addPar("grantBytes");
    grant->par("grantBytes").setLongValue(grantBytes);

    grant->addPar("delay_after_rec_grant");
    grant->par("delay_after_rec_grant")
         .setDoubleValue(delayAfterGrant.dbl());

    grant->addPar("mode");
    grant->par("mode").setLongValue(mode);

    grant->addPar("hostId");
    grant->par("hostId").setLongValue(hostId);

    grant->addPar("lsDuration");
    grant->par("lsDuration").setDoubleValue(lsDuration);

    EV_INFO << "\nOLT sends GATE"
            << "\nONU = " << onuId
            << "\nMode = " << mode
            << "\nHost ID = " << hostId
            << "\nGrant = " << grantBytes << " bytes"
            << "\nDelay after GATE reception = "
            << delayAfterGrant.dbl() * 1e6 << " us"
            << "\nLS duration = " << lsDuration * 1e6 << " us"
            << endl;

    send(grant, "grantOut");
}


// Normal upstream scheduling. The reservation horizon represents
// the expected end of the previous UP reservation at the OLT.
void OLT::sendGrantForONU(int onuId, long grantBytes)
{
    grantBytes = alignedGrantBytes(grantBytes);

    SimTime upstreamPropagation =
        secondsToSimTime(
            oltToRNDelay + rnToONUDelays[onuId]
        );

    SimTime roundTrip =
        secondsToSimTime(
            2.0 * (oltToRNDelay + rnToONUDelays[onuId])
        );

    SimTime earliestArrival = simTime() + roundTrip;

    SimTime arrivalTime =
        std::max(horizonUP, earliestArrival);

    SimTime delayAfterGrant =
        arrivalTime - simTime() - roundTrip;

    sendGrantMessage(
        onuId,
        grantBytes,
        0,
        -1,
        0.0,
        delayAfterGrant
    );

    SimTime dataTxTime =
        secondsToSimTime(grantBytes * 8.0 / dataRate);

    SimTime reportTxTime =
        secondsToSimTime(reportSizeBytes * 8.0 / dataRate);

    horizonUP =
        arrivalTime + dataTxTime + reportTxTime + guardTime;

    EV_INFO << "UP reservation: ONU " << onuId
            << ", first arrival = " << arrivalTime
            << ", reserved until = " << horizonUP
            << ", upstream propagation = " << upstreamPropagation
            << endl;
}


// Rank ONUs and select the six largest reported queues.
void OLT::selectInitialLocalSharingGroup()
{
    if (phase != SchedulerPhase::INITIAL_DISCOVERY)
        return;

    if (initialReportsRemaining != 0)
        throw cRuntimeError(
            "Initial discovery is not complete"
        );

    std::vector<int> rankedONUs(numONUs);

    for (int i = 0; i < numONUs; i++)
        rankedONUs[i] = i;

    std::stable_sort(
        rankedONUs.begin(),
        rankedONUs.end(),
        [this](int a, int b)
        {
            return queueBytes[a] > queueBytes[b];
        }
    );

    initialLSGroup.assign(
        rankedONUs.begin(),
        rankedONUs.begin() + 6
    );

    // The highest reported queue becomes the Host.
    initialHostId = initialLSGroup.front();

    

    EV_INFO << "\n=== INITIAL LOCAL SHARING GROUP ==="
            << "\nHost ONU = " << initialHostId
            << "\nReported queue = "
            << queueBytes[initialHostId] << " bytes"
            << endl;

    for (int rank = 0; rank < 6; rank++)
    {
        int onuId = initialLSGroup[rank];

        EV_INFO << "Rank " << rank + 1
                << ": ONU " << onuId
                << ", queue = " << queueBytes[onuId]
                << " bytes"
                << (rank == 0 ? " (HOST)" : " (CONTRIBUTOR)")
                << endl;
    }

    phase = SchedulerPhase::INITIAL_LOCAL_SHARING;
    scheduleAt(simTime(), scheduleMsg);
}


// Schedule five contributors sequentially on the LS path,
// then schedule the Host after their packets can arrive.
void OLT::scheduleInitialLocalSharing()
{
    if (phase != SchedulerPhase::INITIAL_LOCAL_SHARING)
        return;

    SimTime now = simTime();

    SimTime lsCursor = now;
    SimTime latestLSArrivalAtHost = now;

    long expectedForwardBytes = 0;

    SimTime reportTxTime =
        secondsToSimTime(reportSizeBytes * 8.0 / dataRate);

    // A Mode 1 contributor sends its DATA and then its REPORT
    // through the Local Sharing path.
    for (int rank = 1; rank < 6; rank++)
    {
        int contributorId = initialLSGroup[rank];

        long grantBytes =
            alignedGrantBytes(queueBytes[contributorId]);

        SimTime dataTxTime =
            secondsToSimTime(grantBytes * 8.0 / dataRate);

        SimTime contributorDelay =
            secondsToSimTime(rnToONUDelays[contributorId]);

        SimTime hostDelay =
            secondsToSimTime(rnToONUDelays[initialHostId]);

        // Earliest arrival at the Splitter if the contributor
        // starts as soon as its broadcast GATE reaches it.
        SimTime earliestSplitterArrival =
            now
            + secondsToSimTime(oltToRNDelay)
            + contributorDelay
            + contributorDelay;

        SimTime lsStartAtSplitter =
            std::max(lsCursor, earliestSplitterArrival);

        // GATE reaches ONU after OLT->Splitter and Splitter->ONU.
        // DATA then travels ONU->Splitter.
        SimTime delayAfterGrant =
            lsStartAtSplitter
            - now
            - secondsToSimTime(oltToRNDelay)
            - contributorDelay
            - contributorDelay;

        if (delayAfterGrant < SIMTIME_ZERO)
            delayAfterGrant = SIMTIME_ZERO;

        SimTime contributorBurstDuration =
            dataTxTime + reportTxTime;

        sendGrantMessage(
            contributorId,
            grantBytes,
            1,
            initialHostId,
            contributorBurstDuration.dbl(),
            delayAfterGrant
        );

        // The Host receives the reflected packets after the
        // Splitter->Host propagation delay.
        SimTime reportArrivalAtHost =
            lsStartAtSplitter
            + contributorBurstDuration
            + hostDelay;

        latestLSArrivalAtHost =
            std::max(latestLSArrivalAtHost, reportArrivalAtHost);

        // Reserve the LS wavelength through DATA + REPORT + guard.
        lsCursor =
            lsStartAtSplitter
            + contributorBurstDuration
            + guardTime;

        // Upper bound on bytes the Host must forward.
        expectedForwardBytes += grantBytes + reportSizeBytes;

        EV_INFO << "\nScheduled LS contributor " << contributorId
                << "\nHost = " << initialHostId
                << "\nGrant bytes = " << grantBytes
                << "\nLS start at Splitter = " << lsStartAtSplitter
                << "\nExpected REPORT arrival at Host = "
                << reportArrivalAtHost
                << "\nLS reservation end = " << lsCursor
                << endl;
    }

    horizonLS = lsCursor;

    // The Host can start only after its GATE arrives and all
    // scheduled contributor packets can have reached its LS buffer.
    SimTime hostGateArrival =
        now
        + secondsToSimTime(oltToRNDelay)
        + secondsToSimTime(rnToONUDelays[initialHostId]);

    SimTime hostSendStart =
        std::max(hostGateArrival, latestLSArrivalAtHost);

    // Keep the UP path collision-free as well.
    SimTime hostUpstreamPropagation =
        secondsToSimTime(
            oltToRNDelay + rnToONUDelays[initialHostId]
        );

    if (hostSendStart + hostUpstreamPropagation < horizonUP)
    {
        hostSendStart = horizonUP - hostUpstreamPropagation;

        if (hostSendStart < hostGateArrival)
            hostSendStart = hostGateArrival;
    }

    SimTime hostDelayAfterGrant =
        hostSendStart - hostGateArrival;
    //EV_INFO << "Host GATE arrives at " << hostDelayAfterGrant << " us after GATE reception" << endl;
    if (hostDelayAfterGrant < SIMTIME_ZERO)
        hostDelayAfterGrant = SIMTIME_ZERO;

    // Mode 2 grant covers only the Host's own DATA.
    // Forwarded packets are accounted for separately in time.
    long hostGrantBytes =
        alignedGrantBytes(queueBytes[initialHostId]);

    SimTime hostOwnDataTime =
        secondsToSimTime(hostGrantBytes * 8.0 / dataRate);

    // The Host forwards contributor DATA and REPORT packets
    // sequentially, before sending its own DATA and REPORT.
    SimTime forwardingTime =
        secondsToSimTime(expectedForwardBytes * 8.0 / dataRate);

    SimTime hostFirstArrivalAtOLT =
        hostSendStart + hostUpstreamPropagation;

    horizonUP =
        hostFirstArrivalAtOLT
        + forwardingTime
        + hostOwnDataTime
        + reportTxTime
        + guardTime;

    SimTime totalInitialLSDuration =
        horizonLS - now;

    sendGrantMessage(
        initialHostId,
        hostGrantBytes,
        2,
        -1,
        totalInitialLSDuration.dbl(),
        hostDelayAfterGrant
    );

    EV_INFO << "\n=== INITIAL SCHEDULING COMPLETE ==="
            << "\nNext LS scheduling time = "
            << horizonLS - SimTime(125, SIMTIME_US)
            << "\nUP horizon = " << horizonUP
            << "\nLS horizon = " << horizonLS
            << endl;
    EV_INFO << "\n=== INITIAL LOCAL SHARING SCHEDULED ==="
            << "\nHost = " << initialHostId
            << "\nHost own grant = " << hostGrantBytes << " bytes"
            << "\nExpected forwarded bytes = "
            << expectedForwardBytes
            << "\nExpected Host start = " << hostSendStart
            << "\nLatest contributor REPORT at Host = "
            << latestLSArrivalAtHost
            << "\nLS horizon = " << horizonLS
            << "\nUP horizon = " << horizonUP
            << endl;
}


void OLT::handleMessage(cMessage *msg)
{
    // ----------------------------------------
    // Initial discovery event
    // ----------------------------------------
    
    if (msg == scheduleMsg)
    {
        // ========================================
        // PHASE 1: INITIAL DISCOVERY
        // ========================================
        if (!initialScheduleDone)
        {
            initialScheduleDone = true;
            phase = SchedulerPhase::INITIAL_DISCOVERY;

            EV_INFO << "\n=== INITIAL DISCOVERY ==="
                    << "\nSending zero-byte GATEs to all ONUs"
                    << endl;

            for (int i = 0; i < numONUs; i++)
            {
                sendGrantForONU(i, 0);
            }

            return;
        }

        // ========================================
        // PHASE 2: INITIAL LOCAL SHARING
        // ========================================
        if (phase == SchedulerPhase::INITIAL_LOCAL_SHARING)
        {
            EV_INFO << "\n=== INITIAL LOCAL SHARING ==="
                    << endl;

            // Calculate the initial schedule, send the grants,
            // and establish horizonLS and horizonUP.
            scheduleInitialLocalSharing();

            // Initialization is complete once the schedule has
            // been calculated and the reservations established.
            phase = SchedulerPhase::STEADY_STATE;

            SimTime nextScheduleTime =
                horizonLS - SimTime(125, SIMTIME_US);

            if (nextScheduleTime <= simTime())
            {
                throw cRuntimeError(
                    "Next LS scheduling deadline is not in the future. "
                    "Check the initial LS reservation and 125us lead time."
                );
            }

            EV_INFO << "\nInitial scheduling complete"
                    << "\nNext LS scheduling time = "
                    << nextScheduleTime
                    << "\nHorizon LS = " << horizonLS
                    << "\nHorizon UP = " << horizonUP
                    << endl;

            scheduleAt(nextScheduleTime, scheduleMsg);

            return;
        }

        // ========================================
        // PHASE 3: DYNAMIC LOCAL SHARING
        // ========================================
        if (phase == SchedulerPhase::STEADY_STATE)
        {
            EV_INFO << "\n=== DYNAMIC LOCAL SHARING ==="
                    << "\nScheduling time = " << simTime()
                    << "\nHorizon LS = " << horizonLS
                    << "\nHorizon UP = " << horizonUP
                    << endl;
            return;
            // TODO: Execute the dynamic Local Sharing scheduling
            // algorithm here. It must update horizonLS and horizonUP
            // and issue the appropriate Mode 1 and Mode 2 grants.

            // After dynamic scheduling is implemented, schedule the
            // next decision using the newly calculated horizonLS.
            SimTime nextScheduleTime =
                horizonLS - SimTime(125, SIMTIME_US);

            if (nextScheduleTime <= simTime())
            {
                throw cRuntimeError(
                    "Next dynamic LS scheduling deadline is not "
                    "in the future"
                );
            }

            scheduleAt(nextScheduleTime, scheduleMsg);

            // return;
        }

        return;
    }



    // ----------------------------------------
    // REPORT reception
    // ----------------------------------------
    if (msg->isName("REPORT"))
    {
        if (!msg->hasPar("onuId") ||
            !msg->hasPar("queueBytes"))
        {
            delete msg;
            throw cRuntimeError(
                "REPORT missing onuId or queueBytes"
            );
        }

        int onuId =
            static_cast<int>(msg->par("onuId").longValue());

        long reportedBytes =
            msg->par("queueBytes").longValue();

        if (onuId < 0 || onuId >= numONUs || reportedBytes < 0)
        {
            delete msg;
            throw cRuntimeError("Invalid REPORT contents");
        }

        queueBytes[onuId] = reportedBytes;

        EV_INFO << "\nOLT received REPORT"
                << "\nReported source ONU = " << onuId
                << "\nQueue bytes = " << reportedBytes
                << "\nPhase = " << static_cast<int>(phase)
                << endl;

        if (phase == SchedulerPhase::INITIAL_DISCOVERY)
        {
            if (!initialReportReceived[onuId])
            {
                initialReportReceived[onuId] = true;
                initialReportsRemaining--;

                EV_INFO << "Initial REPORT count remaining = "
                        << initialReportsRemaining
                        << endl;
            }

            delete msg;

            if (initialReportsRemaining == 0)
                selectInitialLocalSharingGroup();

            return;
        }

        if (phase == SchedulerPhase::INITIAL_LOCAL_SHARING)
        {
            // This phase is only for calculating and issuing the initial
            // Local Sharing schedule. It should not wait for the Host REPORT.
            delete msg;
            return;
        }

        // Steady-state normal upstream scheduling.
        delete msg;
        return;
    }

    delete msg;
}


void OLT::finish()
{
    cancelAndDelete(scheduleMsg);
}
