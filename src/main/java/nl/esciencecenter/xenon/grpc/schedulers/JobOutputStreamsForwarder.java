package nl.esciencecenter.xenon.grpc.schedulers;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import nl.esciencecenter.xenon.grpc.XenonProto;

class JobOutputStreamsForwarder {
    
	private final XenonProto.SubmitInteractiveJobResponse.Builder builder;
    private final StreamObserver<XenonProto.SubmitInteractiveJobResponse> observer;

	private static final int BUFFER_SIZE = 1024;
    
    private int streamsDone = 0;
    
    class StreamForwarder extends Thread {
    	
    	private final byte[] buffer = new byte[BUFFER_SIZE];
    
        private final InputStream in;
        
        private boolean stdout = false;
        
        StreamForwarder(InputStream in, boolean stdout) {
        	super("Stream forwarder " + (stdout ? "stdout" : "stderr"));
        	this.in = in;
            this.stdout = stdout;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (true) {

                    int read = in.read(buffer);
                    if (read > 0) {
                    	XenonProto.SubmitInteractiveJobResponse response;
                    	
                    	if (stdout) {
                    		response = builder.clearStderr().setStdout(ByteString.copyFrom(buffer, 0, read)).build();
                    	} else { 
                    		response = builder.clearStdout().setStderr(ByteString.copyFrom(buffer, 0, read)).build();
                    	}
                    	
                    	writeOut(response);

                    } else if (read == -1) {
                        close();
                        return;
                    }                }
            } catch (IOException e) {
            	observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
            }
        }
    }
    
    JobOutputStreamsForwarder(StreamObserver<XenonProto.SubmitInteractiveJobResponse> responseWriter, InputStream stderr, InputStream stdout, XenonProto.Job job) {
        this.observer = responseWriter;
        builder = XenonProto.SubmitInteractiveJobResponse.newBuilder().setJob(job);
        writeOut(builder.build());
        // We should fully read the in and output streams here (non blocking and in parallel) and forward the data to the responseWriter.
        new StreamForwarder(stdout, true).start();
        new StreamForwarder(stderr, false).start();
    }

    private synchronized void writeOut(XenonProto.SubmitInteractiveJobResponse response) {
        observer.onNext(response);
    }

    public synchronized void close() {
    	if (++streamsDone == 2) {
    		observer.onCompleted();
    	}
    }
}
