
#include <omnetpp.h>
#include <cstring>

using namespace omnetpp;

class Splitter : public cSimpleModule
{
  protected:
    virtual void handleMessage(cMessage *msg) override;
};

Define_Module(Splitter);

void Splitter::handleMessage(cMessage *msg)
{
    const char *arrivalGateName = msg->getArrivalGate()->getName();

    // Normal upstream: ONU -> Splitter -> OLT
    if (strcmp(arrivalGateName, "in") == 0)
    {
        send(msg, "out");
        return;
    }

    // Normal downstream: broadcast GATE to every ONU
    if (strcmp(arrivalGateName, "grantIn") == 0)
    {
        for (int i = 0; i < gateSize("grantOut"); i++)
        {
            send(msg->dup(), "grantOut", i);
        }

        delete msg;
        return;
    }

    // Local Sharing: reflect a copy to every ONU
    if (strcmp(arrivalGateName, "lsIn") == 0)
    {
        for (int i = 0; i < gateSize("lsOut"); i++)
        {
            send(msg->dup(), "lsOut", i);
        }

        delete msg;
        return;
    }

    delete msg;
    throw cRuntimeError(
        "Splitter received a message on an unexpected gate");
}
